// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "ConstantImp.h"
#include "Version.h"
#include <sstream>
#include <string>
namespace dolphindb {

class DdbInit {
public:
    DdbInit() {
    #ifdef _WIN32
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2, 1), &wsaData) != 0) {
            throw IOException("Failed to initialize the windows socket.");
        }
    #endif
    }
};

class DBConnectionImpl {
public:
    explicit DBConnectionImpl(bool sslEnable = false, bool asynTask = false, int keepAliveTime = 7200, bool compress = false, bool python = false, bool isReverseStreaming = false, bool enableSCRAM = false);
    ~DBConnectionImpl();
    bool connect(const std::string& hostName, int port, const std::string& userId = "", const std::string& password = "",bool sslEnable = false, bool asynTask = false, int keepAliveTime = -1, bool compress= false, bool python = false);
    void login(const std::string& userId, const std::string& password, bool enableEncryption);
    ConstantSP run(const std::string& script, int priority = 4, int parallelism = 64, int fetchSize = 0, bool clearMemory = false, long seqNum = 0);
    ConstantSP run(const std::string& funcName, std::vector<ConstantSP>& args, int priority = 4, int parallelism = 64, int fetchSize = 0, bool clearMemory = false, long seqNum = 0);
    ConstantSP upload(const std::string& name, const ConstantSP& obj);
    ConstantSP upload(std::vector<std::string>& names, std::vector<ConstantSP>& objs);
    void close();
    bool isConnected() { return isConnected_; }
    void getHostPort(std::string &host, int &port) { host = hostName_; port = port_; }
    void setClientId(const std::string& clientId){runClientId_ = clientId;}
    DataInputStreamSP getDataInputStream(){return inputStream_;}
    VersionT version()
    {
        std::vector<ConstantSP> args;
        std::string str = run("version", args)->getString();
        std::istringstream iss(str);
        VersionT v;
        int tmp;
        char dot;
        iss >> v.major >> dot >> tmp >> dot >> v.minor >> dot >> v.patch;
        constexpr int MAX_VERSION{100};
        v.minor += tmp * MAX_VERSION;
        return v;
    }
    const std::string& getHost() const { return hostName_; }
    int getPort() const { return port_; }
private:
    long generateRequestFlag(bool clearSessionMemory = false, bool disablepickle = false, bool pickleTableToList = false);
    ConstantSP run(const std::string& script, const std::string& scriptType, std::vector<ConstantSP>& args, int priority = 4, int parallelism = 64,int fetchSize = 0, bool clearMemory = false, long seqNum = 0);
    bool connect();
    void login();
    void scramLogin();


    SocketSP conn_;
    std::string sessionId_;
    std::string hostName_;
    int port_;
    std::string userId_;
    std::string pwd_;
    bool encrypted_;
    bool isConnected_;
    bool littleEndian_;
    bool sslEnable_;
    bool enableSCRAM_;
    bool asynTask_;
    int keepAliveTime_;
    bool compress_;
    bool enablePickle_, python_;
    static DdbInit ddbInit_;
    bool isReverseStreaming_;
    std::string runClientId_;
    DataInputStreamSP inputStream_;
};

} // namespace dolphindb