#include "DolphinDBImp.h"
#include "Constant.h"
#include "ConstantMarshall.h"
#include "DolphinDB.h"
#include "Exceptions.h"
#include "Logger.h"
#include "ScalarImp.h"
#include "SmartPointer.h"
#include "SysIO.h"
#include "Types.h"
#include "Util.h"
#include "internal/Crypto.h"
#ifdef USE_OPENSSL
#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#endif

#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

#define APIMinVersionRequirement 300

using std::string;
namespace dolphindb {

DdbInit DBConnectionImpl::ddbInit_;
DBConnectionImpl::DBConnectionImpl(bool sslEnable, bool asynTask, int keepAliveTime, bool compress, bool python, bool isReverseStreaming, bool enableSCRAM)
    : port_(0), encrypted_(false), isConnected_(false), littleEndian_(Util::isLittleEndian()), sslEnable_(sslEnable),enableSCRAM_(enableSCRAM),asynTask_(asynTask)
    , keepAliveTime_(keepAliveTime), compress_(compress), enablePickle_(false), python_(python), isReverseStreaming_(isReverseStreaming)
{
}

DBConnectionImpl::~DBConnectionImpl() {
    close();
    conn_.clear();
}

void DBConnectionImpl::close() {
    if (!isConnected_)
        return;
    isConnected_ = false;
    if (!conn_.isNull()) {
        conn_->close();
    }
}

bool DBConnectionImpl::connect(const std::string & hostName, int port, const std::string & userId,
        const std::string & password, bool sslEnable,bool asynTask, int keepAliveTime, bool compress,
        bool python) {
    hostName_ = hostName;
    port_ = port;
    userId_ = userId;
    pwd_ = password;
    encrypted_ = false;
    sslEnable_ = sslEnable;
    asynTask_ = asynTask;
    if(keepAliveTime > 0){
        keepAliveTime_ = keepAliveTime;
    }
    compress_ = compress;
    python_ = python;
    return connect();
}

bool DBConnectionImpl::connect() {
    close();

    SocketSP conn = new Socket(hostName_, port_, true, keepAliveTime_, sslEnable_);
    IO_ERR ret = conn->connect();
    if (ret != OK) {
        return false;
    }

    string body = "connect\n";
    string out("API 0 ");
    out.append(Util::convert((int)body.size()));
    out.append(" / "+ std::to_string(generateRequestFlag())+"_1_" + std::to_string(4) + "_" + std::to_string(2));
    out.append(1, '\n');
    out.append(body);
    size_t actualLength;
    ret = conn->write(out.c_str(), out.size(), actualLength);
    if (ret != OK)
        throw IOException("Couldn't send login message to the given host/port with IO error type " + std::to_string(ret));

    DataInputStreamSP in = new DataInputStream(conn);
    string line;
    ret = in->readLine(line);
    if (ret != OK)
        throw IOException("Failed to read message from the socket with IO error type " + std::to_string(ret));

    std::vector<string> headers;
    Util::split(line.c_str(), ' ', headers);
    if (headers.size() != 3)
        throw IOException("Received invalid header");
    string sessionId = headers[0];
    int numObject = atoi(headers[1].c_str());
    bool remoteLittleEndian = (headers[2] != "0");

    if ((ret = in->readLine(line)) != OK)
        throw IOException("Failed to read response message from the socket with IO error type " + std::to_string(ret));
    if (line != "OK")
        throw IOException("Server connection response: '" + line);

    if (numObject == 1) {
        uint16_t flag;
        if ((ret = in->readShort(flag)) != OK)
            throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret));
        auto form = static_cast<DATA_FORM>(flag >> 8U);

        ConstantUnmarshallFactory factory(in);
        ConstantUnmarshall* unmarshall = factory.getConstantUnmarshall(form);
        if(unmarshall==nullptr)
            throw IOException("Failed to parse the incoming object" + std::to_string(form));
        if (!unmarshall->start(flag, true, ret)) {
            unmarshall->reset();
            throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret));
        }
        ConstantSP result = unmarshall->getConstant();
        unmarshall->reset();
        if (result->getBool() == 0)
            throw IOException("Failed to authenticate the user");
    }

    conn_ = conn;
    inputStream_ = new DataInputStream(conn_);
    sessionId_ = sessionId;
    isConnected_ = true;
    littleEndian_ = remoteLittleEndian;

    if (!userId_.empty()) {
        try {
            login(userId_, pwd_, HAS_OPENSSL);
        } catch (...) {
            close();
            throw;
        }
    }

    ConstantSP requiredVersion;

    try {
        if(asynTask_) {
            DBConnection newConn(false, false);
            newConn.connect(hostName_, port_, userId_, pwd_);
            requiredVersion = newConn.run("getRequiredAPIVersion()");
        }else{
            requiredVersion = run("getRequiredAPIVersion()");
        }
    }
    catch(...){
        return true;
    }
    if(!requiredVersion->isTuple()){
        return true;
    }
    int apiVersion = requiredVersion->get(0)->getInt();
    if(apiVersion > APIMinVersionRequirement){
        close();
        throw IOException("Required C++ API version at least "  + std::to_string(apiVersion) + ". Current C++ API version is "+ std::to_string(APIMinVersionRequirement) +". Please update DolphinDB C++ API. ");
    }
    if(requiredVersion->size() >= 2 && !requiredVersion->get(1)->getString().empty()){
        std::cout<<requiredVersion->get(1)->getString() <<std::endl;
    }
    return true;
}

void DBConnectionImpl::login(const std::string & userId, const std::string & password, bool enableEncryption) {
    userId_ = userId;
    pwd_ = password;
    if (enableSCRAM_) {
        scramLogin();
    }
    try {
        scramLogin();
    } catch (DataCorruptionException&) {
        throw;
    } catch (...) {
        // try scram login first, if failed use normal login quietly
    }

    encrypted_ = enableEncryption;
    if (encrypted_) {
#ifdef USE_OPENSSL
        std::vector<ConstantSP> args;
        std::string publicKey = run("getDynamicPublicKey", args)->getString();
        if (publicKey.empty()) {
            throw RuntimeException("Failed to obtain RSA public key from server.");
        }
        Crypto ssl(publicKey);
        userId_ = ssl.RSAEncrypt(userId);
        pwd_ = ssl.RSAEncrypt(password);
#else
        throw RuntimeException("Encrypted login is unavailble without OpenSSL.");
#endif
    }
    std::vector<ConstantSP> args;
    args.push_back(new String(userId_));
    args.push_back(new String(pwd_));
    args.push_back(new Bool(encrypted_));
    ConstantSP result = run("login", args);
    if (result->getBool() == 0)
        throw IOException("Failed to authenticate the user " + userId_);
}

void DBConnectionImpl::scramLogin() {
#ifdef USE_OPENSSL
    // call scramClientFirst
    std::vector<ConstantSP> args;
    args.push_back(new String(userId_));
    string clientNonce = Crypto::generateNonce();
    args.push_back(new String(clientNonce));

    ConstantSP result;
    try {
        result = run("scramClientFirst", args);
    } catch (std::exception &e) {
        std::string unsupportStr("Can't recognize function name scramClientFirst");
        std::string wrongAuthModeStr("sha256 authMode doesn't support scram authMode");
        std::string errMsg(e.what());
        if (errMsg.find(unsupportStr) != string::npos) {
            throw IOException("SCRAM login is unavailble on current server.");
        }
        if (errMsg.find(wrongAuthModeStr) != string::npos) {
            throw IOException("user '" + userId_ + "' doesn't support scram authMode.");
        }
        throw;
    }

    if (!result->isVector() || result->size() != 3) {
        throw IOException("SCRAM login failed, server error: get server nonce failed.");
    }

    string saltString = result->get(0)->getString();
    std::vector<uint8_t> salt = Crypto::Base64Decode(saltString, BIO_FLAGS_BASE64_NO_NL);
    int iterCount = result->get(1)->getInt();
    string combinedNonce = result->get(2)->getString();

    // calculate saltedPassword
    std::vector<uint8_t> saltedPassword(SHA256_DIGEST_LENGTH);
    PKCS5_PBKDF2_HMAC(
        pwd_.c_str(), static_cast<int>(pwd_.length()),
        salt.data(), static_cast<int>(salt.size()),
        iterCount,
        EVP_sha256(),
        SHA256_DIGEST_LENGTH, saltedPassword.data()
    );

    // calculate clientKey
    std::vector<uint8_t> clientKey(SHA256_DIGEST_LENGTH);
    HMAC(EVP_sha256(),
        saltedPassword.data(), static_cast<int>(saltedPassword.size()),
        (const uint8_t*)"Client Key", 10,
        clientKey.data(), nullptr);

    // calculate storedKey
    std::vector<uint8_t> storedKey(SHA256_DIGEST_LENGTH);
    SHA256(clientKey.data(), clientKey.size(), storedKey.data());

    // calculate auth
    string authMessage = "n=" + userId_ + ",r=" + clientNonce + ","
        + "r=" + combinedNonce + ",s=" + saltString + ",i=" + std::to_string(iterCount) + ","
        + "c=biws,r=" + combinedNonce;

    // calculate client signature
    std::vector<uint8_t> clientSignature(SHA256_DIGEST_LENGTH);
    HMAC(EVP_sha256(),
        storedKey.data(), static_cast<int>(storedKey.size()),
        (const uint8_t*)authMessage.c_str(), authMessage.size(),
        clientSignature.data(), nullptr);

    // calculate client proof
    std::vector<uint8_t> proof(clientSignature.size());
    for (size_t i = 0; i < clientSignature.size(); ++i) {
        proof[i] = clientKey[i] ^ clientSignature[i];
    }

    // call scramClientFinal
    args.clear();
    args.push_back(new String(userId_));
    args.push_back(new String(combinedNonce));
    args.push_back(new String(Crypto::Base64Encode(proof, BIO_FLAGS_BASE64_NO_NL)));
    result = run("scramClientFinal", args);
    if (!result->isScalar() || result->getType() != DT_STRING) {
        throw IOException("SCRAM login failed, server error: get server signature failed.");
    }

    // calculate serverKey
    std::vector<uint8_t> serverKey(SHA256_DIGEST_LENGTH);
    HMAC(EVP_sha256(),
        saltedPassword.data(), static_cast<int>(saltedPassword.size()),
        (const uint8_t*)"Server Key", 10,
        serverKey.data(), nullptr);

    // check server signature
    std::vector<uint8_t> serverSignature(SHA256_DIGEST_LENGTH);
    HMAC(EVP_sha256(),
        serverKey.data(), static_cast<int>(serverKey.size()),
        (const uint8_t*)authMessage.c_str(), authMessage.length(),
        serverSignature.data(), nullptr);

    string serverSigResult = result->getString();
    if (!serverSigResult.empty() && Crypto::Base64Encode(serverSignature, BIO_FLAGS_BASE64_NO_NL) != serverSigResult) {
        close(); // if check failed, close connection
        throw DataCorruptionException("SCRAM login failed, Invalid server signature");
    }
    DLogger::Debug("SCRAM login succeed.");
#else
    throw RuntimeException("SCRAM login is unavailble without OpenSSL.");
#endif
}


ConstantSP DBConnectionImpl::run(const std::string & script, int priority, int parallelism, int fetchSize, bool clearMemory, long seqNum) {
    std::vector<ConstantSP> args;
    return run(script, "script", args, priority, parallelism, fetchSize, clearMemory, seqNum);
}

ConstantSP DBConnectionImpl::run(const std::string & funcName, std::vector<ConstantSP>& args, int priority, int parallelism, int fetchSize, bool clearMemory, long seqNum) {
    return run(funcName, "function", args, priority, parallelism, fetchSize, clearMemory, seqNum);
}

ConstantSP DBConnectionImpl::upload(const std::string & name, const ConstantSP& obj) {
    if (!Util::isVariableCandidate(name))
        throw RuntimeException(name + " is not a qualified variable name.");
    if (obj.isNull()) {
        throw IllegalArgumentException(DDB_FUNCNAME, "Invalid obj (nullptr).");
    }
    std::vector<ConstantSP> args(1, obj);
    return run(name, "variable", args);
}

ConstantSP DBConnectionImpl::upload(std::vector<string>& names, std::vector<ConstantSP>& objs) {
    if (names.size() != objs.size())
        throw RuntimeException("the size of variable names doesn't match the size of objects.");
    if (names.empty())
        return Constant::void_;

    string varNames;
    for (unsigned int i = 0; i < names.size(); ++i) {
        if (!Util::isVariableCandidate(names[i]))
            throw RuntimeException(names[i] + " is not a qualified variable name.");
        if (i > 0)
            varNames.append(1, ',');
        varNames.append(names[i]);
    }
    return run(varNames, "variable", objs);
}

long DBConnectionImpl::generateRequestFlag(bool clearSessionMemory, bool disablepickle, bool pickleTableToList) {
    long flag = 32; //32 API client
    if (asynTask_){
        flag += 4;
    }
    if (clearSessionMemory){
        flag += 16;
    }
    if (enablePickle_ == false || disablepickle) {
        if (compress_)
            flag += 64;
    }
    else {//enable pickle
        flag += 8;
        if (pickleTableToList) {
            flag += (1 << 15);
        }
    }
    if (python_){
        flag += 2048;
    }
    if(isReverseStreaming_){
        flag += 131072;
    }
    return flag;
}

ConstantSP DBConnectionImpl::run(const std::string & script, const std::string & scriptType, std::vector<ConstantSP>& args,
            int priority, int parallelism, int fetchSize, bool clearMemory, long seqNum) {
    if (!isConnected_)
        throw IOException("Couldn't send script/function to the remote host because the connection has been closed");

    if(fetchSize < 8192 && fetchSize != 0)
        throw IOException("fetchSize must be greater than 8192 and not less than 0");
    string body;
    size_t argCount = args.size();
    if (scriptType == "script")
        body = "script\n" + script;
    else {
        body = scriptType + "\n" + script;
        body.append("\n" + std::to_string(argCount));
        body.append("\n");
        body.append(Util::isLittleEndian() ? "1" : "0");
    }
    string out("API2 " + sessionId_ + " ");
    out.append(Util::convert((int)body.size()));
    out.append(" / " + std::to_string(generateRequestFlag(clearMemory,true)) + "_1_" + std::to_string(priority) + "_" + std::to_string(parallelism));
    out.append("__" + std::to_string(fetchSize));
    if(!runClientId_.empty() && seqNum != 0){
        out.append(std::string("__").append(runClientId_).append("_").append(std::to_string(seqNum)));
    }
    out.append(1, '\n');
    out.append(body);

    IO_ERR ret;
    if (argCount > 0) {
        for (size_t i = 0; i < argCount; ++i) {
            if (args[i]->containNotMarshallableObject()) {
                throw IOException("The function argument or uploaded object is not marshallable.");
            }
        }
        DataOutputStreamSP outStream = new DataOutputStream(conn_);
        ConstantMarshallFactory marshallFactory(outStream);
        bool enableCompress = false;
        for (size_t i = 0; i < argCount; ++i) {
            enableCompress = (args[i]->getForm() == DATA_FORM::DF_TABLE) ? compress_ : false;
            ConstantMarshall* marshall = marshallFactory.getConstantMarshall(args[i]->getForm());
            if (i == 0)
                marshall->start(out.c_str(), out.size(), args[i], true, enableCompress, ret);
            else
                marshall->start(args[i], true, enableCompress, ret);
            marshall->reset();
            if (ret != OK) {
                close();
                throw IOException("Couldn't send function argument to the remote host with IO error type " + std::to_string(ret));
            }
        }
        ret = outStream->flush();
        if (ret != OK) {
            close();
            throw IOException("Failed to marshall code with IO error type " + std::to_string(ret));
        }
    } else {
        size_t actualLength;
        ret = conn_->write(out.c_str(), out.size(), actualLength);
        if (ret != OK) {
            close();
            throw IOException("Couldn't send script/function to the remote host because the connection has been closed, IO error type " + std::to_string(ret));
        }
    }

    if(asynTask_)
        return new Void();

    if (static_cast<int>(littleEndian_) != (char)Util::isLittleEndian())
        inputStream_->enableReverseIntegerByteOrder();

    string line;
    if ((ret = inputStream_->readLine(line)) != OK) {
        close();
        throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret));
    }
    while (line == "MSG") {
        if ((ret = inputStream_->readString(line)) != OK) {
            close();
            throw IOException("Failed to read response msg from the socket with IO error type " + std::to_string(ret));
        }
        std::cout << line << std::endl;
        if ((ret = inputStream_->readLine(line)) != OK) {
            close();
            throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret));
        }
    }
    std::vector<string> headers;
    Util::split(line.c_str(), ' ', headers);
    if (headers.size() != 3) {
        close();
        throw IOException("Received invalid header");
    }
    sessionId_ = headers[0];
    int numObject = atoi(headers[1].c_str());

    if ((ret = inputStream_->readLine(line)) != OK) {
        close();
        throw IOException("Failed to read response message from the socket with IO error type " + std::to_string(ret));
    }

    if (line != "OK") {
        throw IOException(hostName_+":"+std::to_string(port_)+" Server response: '" + line + "' script: '" + script + "'");
    }

    if (numObject == 0) {
        return new Void();
    }

    uint16_t flag;
    if ((ret = inputStream_->readShort(flag)) != OK) {
        close();
        throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret));
    }

    auto form = static_cast<DATA_FORM>(flag >> 8U);
    auto type = static_cast<DATA_TYPE >(flag & 0xffU);
    if(fetchSize > 0 && form == DF_VECTOR && type == DT_ANY)
        return new BlockReader(inputStream_);
    ConstantUnmarshallFactory factory(inputStream_);
    ConstantUnmarshall* unmarshall = factory.getConstantUnmarshall(form);
    if(unmarshall == nullptr){
        DLogger::Error("Unknow incoming object form",form,"of type",type);
        inputStream_->reset(0);
        conn_->skipAll();
        return Constant::void_;
    }
    if (!unmarshall->start(flag, true, ret)) {
        unmarshall->reset();
        close();
        throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret));
    }

    ConstantSP result = unmarshall->getConstant();
    unmarshall->reset();
    return result;
}

} // namespace dolphindb
