/*
 * ScalarImp.cpp
 *
 *  Created on: May 10, 2017
 *      Author: dzhou
 */

#include "ScalarImp.h"
#include "Constant.h"
#include "ConstantImp.h"
#include "Exceptions.h"
#include "Format.h"
#include "SysIO.h"
#include "Types.h"
#include "Util.h"

#include "spdlog/fmt/bundled/format.h"

#ifdef __linux__
#include <uuid/uuid.h>
#else
#include <objbase.h>
#endif

#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <tuple>

namespace dolphindb {

std::string DoubleToString(double val){
    double absVal = std::abs(val);
    if((absVal>0 && absVal<=0.000001) || absVal>=1000000.0) {
        static NumberFormat floatingSciFormat("0.0#####E0");
        return floatingSciFormat.format(val);
    }
    static NumberFormat floatingNormFormat("0.######");
    return floatingNormFormat.format(val);
}

inline int countTemporalUnit(int days, int multiplier, int remainder){
	return days == INT_MIN ? INT_MIN : (days * multiplier) + remainder;
}

inline long long countTemporalUnit(int days, long long multiplier, long long remainder){
	return days == INT_MIN ? LLONG_MIN : (days * multiplier) + remainder;
}

bool Void::isNull(INDEX start, int len, char* buf) const {
	std::ignore = start;
	memset(buf,1,len);
	return true;
}

bool Void::isValid(INDEX start, int len, char* buf) const {
	std::ignore = start;
	memset(buf,0,len);
	return true;
}

bool Void::getBool(INDEX start, int len, char* buf) const {
	std::ignore = start;
	for(int i=0;i<len;++i)
		buf[i]=CHAR_MIN;
	return true;
}

const char* Void::getBoolConst(INDEX start, int len, char* buf) const {
	std::ignore = start;
	for(int i=0;i<len;++i)
		buf[i]=CHAR_MIN;
	return buf;
}

bool Void::getChar(INDEX start, int len, char* buf) const {
	std::ignore = start;
	char tmp=CHAR_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const char* Void::getCharConst(INDEX start, int len, char* buf) const {
	std::ignore = start;
	char tmp=CHAR_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getShort(INDEX start, int len, short* buf) const {
	std::ignore = start;
	short tmp=SHRT_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const short* Void::getShortConst(INDEX start, int len, short* buf) const {
	std::ignore = start;
	short tmp=SHRT_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getInt(INDEX start, int len, int* buf) const {
	std::ignore = start;
	int tmp=INT_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const int* Void::getIntConst(INDEX start, int len, int* buf) const {
	std::ignore = start;
	int tmp=INT_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getLong(INDEX start, int len, long long* buf) const {
	std::ignore = start;
	long long tmp=LLONG_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const long long* Void::getLongConst(INDEX start, int len, long long* buf) const {
	std::ignore = start;
	long long tmp=LLONG_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getIndex(INDEX start, int len, INDEX* buf) const {
	std::ignore = start;
	INDEX tmp=INDEX_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const INDEX* Void::getIndexConst(INDEX start, int len, INDEX* buf) const {
	std::ignore = start;
	INDEX tmp=INDEX_MIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getFloat(INDEX start, int len, float* buf) const {
	std::ignore = start;
	float tmp=FLT_NMIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const float* Void::getFloatConst(INDEX start, int len, float* buf) const {
	std::ignore = start;
	float tmp=FLT_NMIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getDouble(INDEX start, int len, double* buf) const {
	std::ignore = start;
	double tmp=DBL_NMIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const double* Void::getDoubleConst(INDEX start, int len, double* buf) const {
	std::ignore = start;
	double tmp=DBL_NMIN;
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Void::getString(INDEX start, int len, std::string** buf) const {
	std::ignore = start;
	for(int i=0;i<len;++i)
		buf[i]=&Constant::EMPTY;
	return true;
}

std::string** Void::getStringConst(INDEX start, int len, std::string** buf) const {
	std::ignore = start;
	for(int i=0;i<len;++i)
		buf[i]=&Constant::EMPTY;
	return buf;
}

bool Void::getBinary(INDEX start, int len, int unitLength, unsigned char* buf) const {
	std::ignore = start;
	memset(buf, 0, unitLength * len);
	return true;
}

long long Void::getAllocatedMemory() const {return sizeof(Void);}


int Void::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
    std::ignore = bufSize;
    std::ignore = indexStart;
    std::ignore = offset;
	buf[0] = isNothing() ? 0 : 1;
	numElement = 1;
	partial = 0;
	return 1;
}

IO_ERR Void::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) {
    std::ignore = indexStart;
    std::ignore = targetNumElement;
	bool explicitNull;
	IO_ERR ret = in->readBool(explicitNull);
	if(ret == OK)
		numElement = 1;
	setNothing(!explicitNull);
	return ret;
}

int String::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
    std::ignore = indexStart;
    int len = static_cast<int>(val_.size());
    if(len >= 262144){
        throw RuntimeException("String too long, Serialization failed, length must be less than 256K bytes");
    }
    if (!blob_) {
        if (offset > len)
            return -1;
        if (bufSize >= len - offset + 1) {
            numElement = 1;
            partial = 0;
            memcpy(buf, val_.data() + offset, len - offset + 1);
            return len - offset + 1;
        }
        numElement = 0;
        partial = offset + bufSize;
        memcpy(buf, val_.data() + offset, bufSize);
        return bufSize;
    }
	int bytes = 0;
	if (offset > 0) {
		offset -= 4;
		if (UNLIKELY(offset < 0))
			return -1;
	} else {
		if (UNLIKELY((size_t)bufSize < sizeof(int)))
			return 0;
		int sz = static_cast<int>(val_.size());
		memcpy(buf, &sz, sizeof(int));
		buf += sizeof(int);
		bufSize -= sizeof(int);
		bytes += sizeof(int);
	}
	if (bufSize >= len - offset) {
		numElement = 1;
		partial = 0;
		memcpy(buf, val_.data() + offset, len - offset);
		bytes += len - offset;
	} else {
		numElement = 0;
		partial = sizeof(int) + offset + bufSize;
		memcpy(buf, val_.data() + offset, bufSize);
		bytes += bufSize;
	}
	return bytes;
}

IO_ERR String::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
    std::ignore = indexStart;
    std::ignore = targetNumElement;
    IO_ERR ret;
    if (blob_) {
        int len;
        if ((ret = in->readInt(len)) != OK)
            return ret;
        std::unique_ptr<char[]> buf(new char[len]);
        if ((ret = in->read(buf.get(), len)) != OK)
            return ret;
        val_.clear();
        val_.append(buf.get(), len);
    } else {
        ret = in->readString(val_);
        if (ret == OK)
            numElement = 1;
    }
    return ret;
}

Bool* Bool::parseBool(const std::string & str){
	Bool* p=nullptr;

	if(str=="00"){
		p=new Bool();
		p->setNull();
	}
	else if(Util::equalIgnoreCase(str,"true")){
		p=new Bool(true);
	}
	else if(Util::equalIgnoreCase(str,"false")){
		p=new Bool(false);
	}
	else{
		p=new Bool(atoi(str.c_str()) != 0);
	}
	return p;
}

IO_ERR Bool::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
    std::ignore = indexStart;
    std::ignore = targetNumElement;
	IO_ERR ret = in->readChar(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}

std::string Char::toString(char val) {
	if(val == CHAR_MIN)
		return "";
	if(val>31 && val<127)
		return std::string(1, val);
	return NumberFormat::toString(val);
}

std::string Char::getScript() const {
	if(isNull())
		return "00c";
	if(val_>31 && val_<127){
		std::string str("' '");
		str[1] = val_;
		return str;
	}
	char buf[5];
#ifdef _MSC_VER
	sprintf_s(buf, 5, "%d", val_);
#else
	sprintf(buf, "%d", val_);
#endif
	return std::string(buf);
}

Char* Char::parseChar(const std::string & str){
	Char* p=nullptr;
	if(str=="00" || str.empty()){
		p=new Char();
		p->setNull();
	}
	else if(str[0]=='\''){
		char ch=CHAR_MIN;
		if(str.size()==4 && str[3]=='\'' && str[1]=='\\'){
			ch=Util::escape(str[2]);
			if(ch==0)
				ch=str[2];
		} else if(str.size()==3 && str[2]=='\'')
			ch=str[1];
		p=new Char(ch);
	}
	else{
		int val = atoi(str.c_str());
		if(val>127 || val < -128)
			return nullptr;
		p=new Char(static_cast<char>(atoi(str.c_str())));
	}
	return p;
}

IO_ERR Char::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
    std::ignore = indexStart;
    std::ignore = targetNumElement;
	IO_ERR ret = in->readChar(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}

Short* Short::parseShort(const std::string & str){
	Short* p=nullptr;

	if(str=="00"){
		p=new Short();
		p->setNull();
	}
	else{
		int val = atoi(str.c_str());
		if(val>65535 || val < -65536)
			return nullptr;
		p=new Short(static_cast<short>(atoi(str.c_str())));
	}
	return p;
}

std::string Short::toString(short val){
	if(val == SHRT_MIN)
		return "";
	return NumberFormat::toString(val);
}

IO_ERR Short::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
    std::ignore = indexStart;
    std::ignore = targetNumElement;
	IO_ERR ret = in->readShort(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}

Int* Int::parseInt(const std::string & str){
	Int* p=nullptr;
	if(str=="00"){
		p=new Int();
		p->setNull();
	}
	else
		p=new Int(atoi(str.c_str()));
	return p;
}

std::string Int::toString(int val){
	if(val == INT_MIN)
		return "";
	return NumberFormat::toString(val);
}

IO_ERR Int::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
    std::ignore = indexStart;
    std::ignore = targetNumElement;
	IO_ERR ret = in->readInt(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}

Long* Long::parseLong(const std::string & str){
	Long* p=nullptr;

	if(str=="00"){
		p=new Long();
		p->setNull();
	}
	else
		p=new Long(atoll(str.c_str()));
	return p;
}

std::string Long::toString(long long val){
	if(val == LLONG_MIN)
		return "";
	return NumberFormat::toString(val);
}

IO_ERR Long::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
    std::ignore = indexStart;
    std::ignore = targetNumElement;
	IO_ERR ret = in->readLong(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}


Int128::Int128(const unsigned char* data){
	memcpy(uuid_, data, 16);
}

Int128::Int128(){
}

int Int128::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
    std::ignore = indexStart;
	int len = 16 - offset;
	if(len < 0)
		return -1;
	if(bufSize >= len){
		numElement = 1;
		partial = 0;
		memcpy(buf,((char*)uuid_)+offset, len);
		return len;
	}
	len = bufSize;
	numElement = 0;
	partial = offset + bufSize;
	memcpy(buf, ((char *)uuid_) + offset, len);
	return len;
}

IO_ERR Int128::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) {
    std::ignore = indexStart;
    std::ignore = targetNumElement;
	IO_ERR ret = in->readBytes((char*)uuid_, 16, false);
	if(ret == OK)
		numElement = 1;
	return ret;
}

bool Int128::isNull() const {
	const auto* a = (const unsigned char*)uuid_;
	return (*(long long*)a) == 0 && (*(long long*)(a+8)) == 0;
}

void Int128::setNull(){
	memset((void*)uuid_, 0, 16);
}

void Int128::setBinary(const unsigned char* val, int unitLength){
    std::ignore = unitLength;
	memcpy(uuid_, val, 16);
}

bool Int128::getBinary(INDEX start, int len, int unitLength, unsigned char* buf) const{
    std::ignore = start;
	if(unitLength != 16)
		return false;
	for(int i=0; i<len; ++i){
		memcpy(buf, uuid_, 16);
		buf += 16;
	}
	return true;
}

const unsigned char* Int128::getBinaryConst(INDEX start, int len, int unitLength, unsigned char* buf) const {
    std::ignore = start;
    std::ignore = unitLength;
	unsigned char* original = buf;
	for(int i=0; i<len; ++i){
		memcpy(buf, uuid_, 16);
		buf += 16;
	}
	return original;
}

std::string Int128::toString(const unsigned char* data) {
	char buf[32];
	Util::toHex(data, 16, Util::LITTLE_ENDIAN_ORDER, buf);
	return std::string(buf, 32);
}

Int128* Int128::parseInt128(const char* str, size_t len){
	unsigned char buf[16];
	if(len == 0){
		memset(buf, 0, 16);
		return new Int128(buf);
	}
	if(len == 32 && Util::fromHex(str, len, Util::LITTLE_ENDIAN_ORDER, buf))
		return new Int128(buf);
	return nullptr;
}

bool Int128::parseInt128(const char* str, size_t len, unsigned char *buf) {
	if (len == 0) {
		memset(buf, 0, 16);
		return true;
	}
	if (len == 32 && Util::fromHex(str, len, Util::LITTLE_ENDIAN_ORDER, buf))
		return true;
	return false;
}

int Int128::compare(INDEX index, const ConstantSP& target) const {
    std::ignore = index;
	return guid_.compare(target->getInt128());
}

Uuid::Uuid(bool newUuid){
	if(!newUuid){
		memset(uuid_, 0, 16);
	}
	else{
#ifdef _WIN32
	CoCreateGuid((GUID*)uuid_);
#else
	uuid_generate(uuid_);
#endif
	}
}

Uuid::Uuid(const unsigned char* uuid){
	memcpy(uuid_, uuid, 16);
}

Uuid::Uuid(const char* guid, size_t len){
	if(len == 0)
		memset(uuid_, 0, 16);
	else if(len != 36 || !Util::fromGuid(guid, uuid_))
		throw RuntimeException("Invalid UUID string");
}

Uuid::Uuid(const Uuid& copy)
	: Int128()
{
	memcpy(uuid_, copy.uuid_, 16);
}

Uuid* Uuid::parseUuid(const char* str, size_t len){
	return new Uuid(str, len);
}

bool Uuid::parseUuid(const char* str, size_t len, unsigned char *buf){
	if(len == 0)
		memset(buf, 0, 16);
	else if(len != 36 || !Util::fromGuid(str, buf))
		return false;
	return true;
}

/**
 * Assume big-endian encoding for ipv4 and ipv6
 */
IPAddr::IPAddr(const char* ip, int len){
	if(len == 0 || !parseIPAddr(ip, len, uuid_))
		memset(uuid_, 0, 16);
}

IPAddr::IPAddr(const unsigned char* data) : Int128(data){
}

/**
 * Assume big-endian encoding for ipv4 and ipv6
 */
std::string IPAddr::toString(const unsigned char* data) {
	char buf[40];
	int cursor = 0;
	bool isIP4;

	if(LIKELY(Util::LITTLE_ENDIAN_ORDER))
		isIP4 = *(unsigned long long*)(data + 8) == 0 && *(unsigned int*)(data+4) == 0;
	else
		isIP4 = *(unsigned long long*)data == 0 && *(unsigned int*)(data+8) == 0;
	if(isIP4){
		if(LIKELY(Util::LITTLE_ENDIAN_ORDER)){
			for(int i=3; i>=0; --i){
#ifdef _MSC_VER
				cursor += sprintf_s(buf + cursor, 40 - cursor, "%d", data[i]);
#else
				cursor += sprintf(buf + cursor, "%d", data[i]);
#endif
				buf[cursor++] = '.';
			}
		} else{
			for(int i=12; i<16; ++i){
#ifdef _MSC_VER
				cursor += sprintf_s(buf + cursor, 40 - cursor, "%d", data[i]);
#else
				cursor += sprintf(buf + cursor, "%d", data[i]);
#endif
				buf[cursor++] = '.';
			}
		}
	}
	else{
		bool consecutiveZeros = false;
		for(int i=0; i<16; i=i+2){
			if(i > 0 && i<12 && !consecutiveZeros && *(unsigned int*)(data + (Util::LITTLE_ENDIAN_ORDER ? 11 -i : i)) == 0){
				//consecutive zeros
				consecutiveZeros = true;
				i += 2;
				while(i<12 && *(unsigned int*)(data + (Util::LITTLE_ENDIAN_ORDER ? 11 -i : i)) == 0) i += 2;
			}
			else{
				bool skipZero = true;
				unsigned char ch = data[Util::LITTLE_ENDIAN_ORDER ? 15 - i : i];
				if(ch > 0){
					if(ch>15){
						unsigned char high = ch >> 4;
						buf[cursor++] = high >= 10 ? high + 87 : high + 48;
						ch &= 15;
					}
					buf[cursor++] = ch >= 10 ? ch + 87 : ch + 48;
					skipZero = false;
				}
				ch = data[Util::LITTLE_ENDIAN_ORDER ? 14 - i : i + 1];
				if(!skipZero || ch>15){
					unsigned char high = ch >> 4;
					buf[cursor++] = high >= 10 ? high + 87 : high + 48;
					ch &= 15;
				}
				buf[cursor++] = ch >= 10 ? ch + 87 : ch + 48;
			}
			buf[cursor++] = ':';
		}
	}
	return std::string(buf, cursor - 1);
}

IPAddr* IPAddr::parseIPAddr(const char* str, size_t len){
	unsigned char buf[16];
	if(parseIPAddr(str, len, buf))
		return new IPAddr(buf);
	return nullptr;
}

bool IPAddr::parseIPAddr(const char* str, size_t len, unsigned char* buf){
	if(len < 7)
		return false;
	int i = 0;
	for(i=0; i<4 && str[i] != '.'; ++i);
	if(i >= 4)
		return parseIP6(str, len, buf);
	return parseIP4(str, len, buf);
}

bool IPAddr::parseIP4(const char* str, size_t len, unsigned char* buf){
	int byteIndex = 0;
	int curByte = 0;

	for(size_t i=0; i<=len; ++i){
		if(i==len || str[i] == '.'){
			if(curByte < 0 || curByte > 255 || byteIndex > 3)
				return false;
			buf[Util::LITTLE_ENDIAN_ORDER ? 3 - byteIndex++ : 12 + byteIndex++ ] = static_cast<unsigned char>(curByte);
			curByte = 0;
			continue;
		}
		if(str[i] < '0' || str[i] > '9')
			return false;
		curByte = curByte * 10 + str[i] - 48;
	}
	if(byteIndex != 4)
		return false;
	memset(buf + (Util::LITTLE_ENDIAN_ORDER ? 4 : 0), 0, 12);
	return true;
}

bool IPAddr::parseIP6(const char* str, size_t len, unsigned char* buf){
	int byteIndex = 0;
	size_t curByte = 0;
	int lastColonPos = -1;
	for(size_t i=0; i<=len; ++i){
		if(i==len || str[i] == ':'){
			//check consecutive colon
			if(lastColonPos == (int)i - 1){
				//check how many colons in the remaining string
				int colons = byteIndex/2;
				for(size_t k=i+1; k<len; ++k)
					if(str[k] ==':')
						++colons;
				int consecutiveZeros = (7 - colons)*2;
				if(Util::LITTLE_ENDIAN_ORDER){
					for(int k=0; k<consecutiveZeros; ++k)
						buf[15 - byteIndex++] = 0;
				}
				else{
					for(int k=0; k<consecutiveZeros; ++k)
						buf[byteIndex++] = 0;
				}
			}
			else{
				if(curByte > 65535 || byteIndex > 15)
					return false;
				if(Util::LITTLE_ENDIAN_ORDER){
					buf[15 - byteIndex++] = static_cast<unsigned char>(curByte>>8);
					buf[15 - byteIndex++] = curByte & 255U;
				}
				else{
					buf[byteIndex++] = static_cast<unsigned char>(curByte>>8);
					buf[byteIndex++] = curByte & 255;
				}
				curByte = 0;
			}
			lastColonPos = static_cast<int>(i);
			continue;
		}
		char ch = str[i];
		char value = ch >= 97 ? ch -87 : (ch >= 65 ? ch -55 : ch -48);
		if(value < 0 || value > 15)
			return false;
		curByte = (curByte<<4) + value;
	}
	return byteIndex==16;
}

bool Float::getChar(INDEX start, int len, char* buf) const {
    std::ignore = start;
	char tmp = isNull() ? CHAR_MIN : static_cast<char>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const char* Float::getCharConst(INDEX start, int len, char* buf) const {
    std::ignore = start;
	char tmp = isNull() ? CHAR_MIN : static_cast<char>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Float::getShort(INDEX start, int len, short* buf) const {
    std::ignore = start;
	short tmp = isNull() ? SHRT_MIN : static_cast<short>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const short* Float::getShortConst(INDEX start, int len, short* buf) const {
    std::ignore = start;
	short tmp = isNull() ? SHRT_MIN : static_cast<short>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Float::getInt(INDEX start, int len, int* buf) const {
    std::ignore = start;
	int tmp = isNull() ? INT_MIN : static_cast<int>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const int* Float::getIntConst(INDEX start, int len, int* buf) const {
    std::ignore = start;
	int tmp = isNull() ? INT_MIN : static_cast<int>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Float::getLong(INDEX start, int len, long long* buf) const {
    std::ignore = start;
	long long tmp = isNull() ? LLONG_MIN : static_cast<long long>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const long long* Float::getLongConst(INDEX start, int len, long long* buf) const {
    std::ignore = start;
	long long tmp = isNull() ? LLONG_MIN : static_cast<long long>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

std::string Float::toString(float val){
    if(val == FLT_NMIN)
        return "";
    if(std::isnan(val))
        return "NaN";
    if(std::isinf(val))
        return "inf";
    return DoubleToString(val);
}

Float* Float::parseFloat(const std::string & str){
	Float* p=nullptr;

	if(str=="00"){
		p=new Float();
		p->setNull();
	}
	else
		p=new Float(static_cast<float>(atof(str.c_str())));
	return p;
}

IO_ERR Float::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
    std::ignore = indexStart;
    std::ignore = targetNumElement;
	IO_ERR ret = in->readFloat(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}

bool Double::getChar(INDEX start, int len, char* buf) const {
    std::ignore = start;
	char tmp = isNull() ? CHAR_MIN : static_cast<char>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const char* Double::getCharConst(INDEX start, int len, char* buf) const {
    std::ignore = start;
	char tmp = isNull() ? CHAR_MIN : static_cast<char>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Double::getShort(INDEX start, int len, short* buf) const {
    std::ignore = start;
	short tmp = isNull() ? SHRT_MIN : static_cast<short>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const short* Double::getShortConst(INDEX start, int len, short* buf) const {
    std::ignore = start;
	short tmp = isNull() ? SHRT_MIN : static_cast<short>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Double::getInt(INDEX start, int len, int* buf) const {
    std::ignore = start;
	int tmp = isNull() ? INT_MIN : static_cast<int>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}

const int* Double::getIntConst(INDEX start, int len, int* buf) const {
    std::ignore = start;
	int tmp = isNull() ? INT_MIN : static_cast<int>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

bool Double::getLong(INDEX start, int len, long long* buf) const {
    std::ignore = start;
	long long tmp = isNull() ? LLONG_MIN : static_cast<long long>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return true;
}
const long long* Double::getLongConst(INDEX start, int len, long long* buf) const {
    std::ignore = start;
	long long tmp = isNull() ? LLONG_MIN : static_cast<long long>(val_ < 0 ? (val_ - 0.5) : (val_ + 0.5));
	for(int i=0;i<len;++i)
		buf[i]=tmp;
	return buf;
}

std::string Double::toString(double val){
    if(val == DBL_NMIN)
        return "";
    if(std::isnan(val))
        return "NaN";
    if(std::isinf(val))
        return "inf";
    return DoubleToString(val);
}

Double* Double::parseDouble(const std::string & str){
	Double* p=nullptr;

	if(str=="00"){
		p=new Double();
		p->setNull();
	}
	else
		p=new Double(atof(str.c_str()));
	return p;
}

IO_ERR Double::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement){
    std::ignore = indexStart;
    std::ignore = targetNumElement;
	IO_ERR ret = in->readDouble(val_);
	if(ret == OK)
		numElement = 1;
	return ret;
}

Date* Date::parseDate(const std::string & date){
	//format: 2012.09.01
	Date* p=nullptr;

	if(date=="00"){
		p=new Date();
		p->setNull();
		return p;
	}

	int year,month,day;
	year=atoi(date.substr(0,4).c_str());
	if(year==0)
		return p;
	if(date[4]!='.')
		return p;
	month=atoi(date.substr(5,2).c_str());
	if(month==0)
		return p;
	if(date[7]!='.')
		return p;
	if(date[8]=='0')
		day=atoi(date.substr(9,1).c_str());
	else
		day=atoi(date.substr(8,2).c_str());
	if(day==0)
		return p;
	p=new Date(year,month,day);
	return p;
}

std::string Date::toString(int val){
    if(val == INT_MIN)
        return "";
    int y;
    int m;
    int d;
    Util::parseDate(val, y, m, d);
    return fmt::format("{:04d}.{:02d}.{:02d}", y, m, d);
}

DateTime::DateTime(int year, int month, int day, int hour, int minute, int second)
    : TemporalScalar(countTemporalUnit(Util::countDays(year, month, day), 86400, ((hour * 60 + minute) * 60) + second)) {}

std::string DateTime::toString(int val){
    if(val == INT_MIN)
        return "";
    static TemporalFormat datetimeFormat("yyyy.MM.ddTHH:mm:ss");
    return datetimeFormat.format(val, DT_DATETIME);
}

std::string Month::toString(int val){
    if(val == INT_MIN)
        return "";
    static TemporalFormat monthFormat("yyyy.MM\\M");
    return monthFormat.format(val, DT_MONTH);
}

std::string Time::toString(int val){
    if(val < 0 || val >= 86400000)
        return "";
    static TemporalFormat timeFormat("HH:mm:ss.SSS");
    return timeFormat.format(val, DT_TIME);
}

void Time::validate(){
	if(val_ >= 86400000 || val_ <0)
		val_ = INT_MIN;
}

std::string Minute::toString(int val){
	if(val < 0 || val >= 1440)
		return "";
    static TemporalFormat minuteFormat("HH:mm\\m");
	return minuteFormat.format(val, DT_MINUTE);
}

void Minute::validate(){
	if(val_ >= 1440 || val_ <0)
		val_ = INT_MIN;
}

std::string Second::toString(int val){
	if(val < 0 || val >= 86400 )
		return "";
    static TemporalFormat secondFormat("HH:mm:ss");
	return secondFormat.format(val, DT_SECOND);
}

void Second::validate(){
	if(val_ >= 86400 || val_ <0)
		val_ = INT_MIN;
}

Timestamp::Timestamp(int year, int month, int day, int hour, int minute, int second, int milliSecond)
    : Long(countTemporalUnit(Util::countDays(year, month, day), 86400000LL,
                            (((hour * 60 + minute) * 60 + second) * 1000LL) + milliSecond)) {}

std::string Timestamp::toString(long long val){
	if(val == LLONG_MIN)
		return "";
    static TemporalFormat timestampFormat("yyyy.MM.ddTHH:mm:ss.SSS");
	return timestampFormat.format(val, DT_TIMESTAMP);
}

NanoTimestamp::NanoTimestamp(int year, int month, int day,int hour, int minute, int second, int nanoSecond):
	Long(countTemporalUnit(Util::countDays(year,month,day), 86400000000000LL, (((hour*60+minute)*60+second)*1000000000LL)+nanoSecond)){
}

std::string NanoTimestamp::toString(long long val){
	if(val == LLONG_MIN)
		return "";
    static TemporalFormat nanotimestampFormat("yyyy.MM.ddTHH:mm:ss.nnnnnnnnn");
	return nanotimestampFormat.format(val, DT_NANOTIMESTAMP);
}

std::string NanoTime::toString(long long val){
    if(val < 0 || val >= 86400000000000LL)
        return "";
    static TemporalFormat nanotimeFormat("HH:mm:ss.nnnnnnnnn");
    return nanotimeFormat.format(val, DT_NANOTIME);
}

void NanoTime::validate(){
	if(val_ >= 86400000000000LL || val_ <0)
		val_ = LLONG_MIN;
}

Month* Month::parseMonth(const std::string& str){
	//2012.01
	Month* p=nullptr;

	if(str=="00"){
		p=new Month();
		p->setNull();
		return p;
	}
	if(str.length()!=7)
		return p;

	int year,month;
	year=atoi(str.substr(0,4).c_str());
	if(year==0)
		return p;
	if(str[4]!='.')
		return p;
	month=atoi(str.substr(5,2).c_str());
	if(month==0 || month>12)
		return p;

	p=new Month(year,month);
	return p;
}

Time* Time::parseTime(const std::string& str){
	//23:04:59:001
	Time* p=nullptr;
	if(str=="00"){
		p=new Time();
		p->setNull();
		return p;
	}
	if(str.size() != 12)
		return p;
	int hour,minute,second,milliSecond=0;
	hour=atoi(str.substr(0,2).c_str());
	if(hour>=24 || str[2]!=':')
		return p;
	minute=atoi(str.substr(3,2).c_str());
	if(minute>=60 || str[5]!=':')
		return p;
	second=atoi(str.substr(6,2).c_str());
	if(second>=60)
		return p;
	if(str[8]=='.')
		milliSecond=atoi(str.substr(9,str.length()-9).c_str());
	p=new Time(hour,minute,second,milliSecond);
	return p;
}

NanoTime* NanoTime::parseNanoTime(const std::string& str){
	//23:04:59:000000001
	NanoTime* p=nullptr;
	if(str=="00"){
		p=new NanoTime();
		p->setNull();
		return p;
	}

	int hour,minute,second,nanoSecond=0;
	hour=atoi(str.substr(0,2).c_str());
	if(hour>=24 || str[2]!=':')
		return p;
	minute=atoi(str.substr(3,2).c_str());
	if(minute>=60 || str[5]!=':')
		return p;
	second=atoi(str.substr(6,2).c_str());
	if(second>=60)
		return p;
	if(str[8]=='.'){
		std::size_t len  = str.length()-9;
		if(len == 9)
			nanoSecond=atoi(str.substr(9, len).c_str());
		else if(len == 6)
			nanoSecond=atoi(str.substr(9, len).c_str()) * 1000;
		else if(len == 3)
			nanoSecond=atoi(str.substr(9, len).c_str()) * 1000000;
		else
			return p;
	}
	p=new NanoTime(hour,minute,second,nanoSecond);
	return p;
}

Minute* Minute::parseMinute(const std::string& str){
	Minute* p=nullptr;
	if(str=="00"){
		p=new Minute();
		p->setNull();
		return p;
	}

	int hour,minute;
	hour=atoi(str.substr(0,2).c_str());
	if(hour>=24 || str[2]!=':')
		return p;
	minute=atoi(str.substr(3,2).c_str());
	if(minute>=60)
		return p;
	p=new Minute(hour,minute);
	return p;
}

Second* Second::parseSecond(const std::string& str){
	Second* p=nullptr;
	if(str=="00"){
		p=new Second();
		p->setNull();
		return p;
	}

	int hour,minute,second;
	hour=atoi(str.substr(0,2).c_str());
	if(hour>=24 || str[2]!=':')
		return p;
	minute=atoi(str.substr(3,2).c_str());
	if(minute>=60 || str[5]!=':')
		return p;
	second=atoi(str.substr(6,2).c_str());
	if(second>=60)
		return p;
	p=new Second(hour,minute,second);
	return p;
}

template <class T>
template <typename R>
bool AbstractScalar<T>::getDecimal(INDEX /*start*/, int len, int scale, R *buf) const {
	R value{};
	decimal_util::valueToDecimalraw(val_, scale, &value);
	for (int i = 0; i < len; ++i) {
		buf[i] = value;
	}
	return true;
}

DateTime* DateTime::parseDateTime(const std::string& str){
	DateTime* p=nullptr;

	if(str=="00"){
		p=new DateTime();
		p->setNull();
		return p;
	}

	int year,month,day,hour,minute,second;
	year=atoi(str.substr(0,4).c_str());
	if(year==0 ||str[4]!='.')
		return p;
	month=atoi(str.substr(5,2).c_str());
	if(month==0 || str[7]!='.')
		return p;
	day=atoi(str.substr(8,2).c_str());
	if(day==0 || (str[10]!=' ' && str[10]!='T'))
		return p;

	hour=atoi(str.substr(11,2).c_str());
	if(hour>=24 || str[13]!=':')
		return p;
	minute=atoi(str.substr(14,2).c_str());
	if(minute>=60 || str[16]!=':')
		return p;
	second=atoi(str.substr(17,2).c_str());
	if(second>=60)
		return p;
	p=new DateTime(year,month,day,hour,minute,second);
	return p;
}

Timestamp* Timestamp::parseTimestamp(const std::string& str){
	Timestamp* p=nullptr;

	if(str=="00"){
		p=new Timestamp();
		p->setNull();
		return p;
	}

	std::size_t len = str.size();
	if(len < 19)
		return p;

	int year,month,day,hour,minute,second,millisecond=0;
	year=atoi(str.substr(0,4).c_str());
	if(year==0 ||str[4]!='.')
		return p;
	month=atoi(str.substr(5,2).c_str());
	if(month==0 || str[7]!='.')
		return p;
	day=atoi(str.substr(8,2).c_str());
	if(day==0 || (str[10]!=' ' && str[10]!='T'))
		return p;

	hour=atoi(str.substr(11,2).c_str());
	if(hour>=24 || str[13]!=':')
		return p;
	minute=atoi(str.substr(14,2).c_str());
	if(minute>=60 || str[16]!=':')
		return p;
	second=atoi(str.substr(17,2).c_str());
	if(second>=60)
		return p;
	if(len > 19 && str[19]=='.'){
		if(len > 22)
			millisecond=atoi(str.substr(20, 3).c_str());
		else
			return p;
	}
	p=new Timestamp(year,month,day,hour,minute,second,millisecond);
	return p;
}

NanoTimestamp* NanoTimestamp::parseNanoTimestamp(const std::string& str){
	NanoTimestamp* p=nullptr;

	if(str=="00"){
		p=new NanoTimestamp();
		p->setNull();
		return p;
	}

	int year,month,day,hour,minute,second,nanosecond=0;
	year=atoi(str.substr(0,4).c_str());
	if(year==0 ||str[4]!='.')
		return p;
	month=atoi(str.substr(5,2).c_str());
	if(month==0 || str[7]!='.')
		return p;
	day=atoi(str.substr(8,2).c_str());
	if(day==0 || (str[10]!=' ' && str[10]!='T'))
		return p;

	hour=atoi(str.substr(11,2).c_str());
	if(hour>=24 || str[13]!=':')
		return p;
	minute=atoi(str.substr(14,2).c_str());
	if(minute>=60 || str[16]!=':')
		return p;
	second=atoi(str.substr(17,2).c_str());
	if(second>=60)
		return p;
	if(str[19]=='.'){
		std::size_t len = str.length()-20;
		if(len == 9)
			nanosecond=atoi(str.substr(20, len).c_str());
		else if(len == 6)
			nanosecond=atoi(str.substr(20, len).c_str()) * 1000;
		else if(len == 3)
			nanosecond=atoi(str.substr(20, len).c_str()) * 1000000;
		else
			return p;
	}
	p=new NanoTimestamp(year,month,day,hour,minute,second,nanosecond);
	return p;
}

DateHour::DateHour(int year, int month, int day, int hour):
    TemporalScalar(countTemporalUnit(Util::countDays(year,month,day),24,hour)){
}

std::string DateHour::toString(int val){
    if(val == INT_MIN)
        return "";
    static TemporalFormat datehourFormat("yyyy.MM.ddTHH");
    return datehourFormat.format(val, DT_DATEHOUR);
}

DateHour* DateHour::parseDateHour(const std::string& str){
    DateHour* p=nullptr;

    if(str=="00"){
        p=new DateHour();
        p->setNull();
        return p;
    }

    if(UNLIKELY(str.size() < 13))
        return p;
    int year,month,day,hour;
    year=atoi(str.substr(0,4).c_str());
    if(year==0 ||str[4]!='.')
        return p;
    month=atoi(str.substr(5,2).c_str());
    if(month==0 || str[7]!='.')
        return p;
    day=atoi(str.substr(8,2).c_str());
    if(day==0 || (str[10]!=' ' && str[10]!='T'))
        return p;

    hour=atoi(str.substr(11,2).c_str());
    if(hour>=24)
        return p;
    p=new DateHour(year,month,day,hour);
    return p;
}

ConstantSP Date::castTemporal(DATA_TYPE expectType) {
	if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
		throw RuntimeException("castTemporal from DATE to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType != DT_DATE && expectType != DT_TIMESTAMP && expectType != DT_NANOTIMESTAMP && expectType != DT_MONTH && expectType != DT_DATETIME && expectType != DT_DATEHOUR) {
		throw RuntimeException("castTemporal from DATE to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_DATE)
		return getValue();

	if (expectType == DT_DATEHOUR) {
		int result;
		val_ == INT_MIN ? result = INT_MIN : result = val_ * 24;
		return Util::createObject(expectType, result);
	}
	long long ratio = Util::getTemporalConversionRatio(DT_DATE, expectType);
	if (expectType == DT_NANOTIMESTAMP || expectType == DT_TIMESTAMP) {
		long long result;

		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio;
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_DATETIME) {
		int result;

		val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ * ratio);
		return Util::createObject(expectType, result);
	}
	int result;

	if (val_ == INT_MIN) {
		result = INT_MIN;
	} else {
		int year, month, day;
		Util::parseDate(val_, year, month, day);
		result = year * 12 + month - 1;
	}
	return Util::createObject(expectType, result);
}
ConstantSP DateHour::castTemporal(DATA_TYPE expectType) {
	if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
		throw RuntimeException("castTemporal from DATEHOUR to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_DATEHOUR)
		return getValue();

	long long ratio = Util::getTemporalConversionRatio(DT_DATETIME, expectType);
	if (expectType == DT_NANOTIMESTAMP || expectType == DT_TIMESTAMP) {
		long long result;
		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio * 3600;
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_DATETIME) {
		int result;
		val_ == INT_MIN ? result = INT_MIN : result = val_ * 3600;
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_DATE) {
		int result;
		val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ * 3600 / (-ratio));
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_MONTH) {
		int result;
		if (val_ == INT_MIN) {
			result = INT_MIN;
		} else {
			int year, month, day;
			Util::parseDate(val_ * 3600 / 86400, year, month, day);
			result = year * 12 + month - 1;
		}
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_NANOTIME) {
		long long result;
		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * 3600 % 86400 * 1000000000LL;
		return Util::createObject(expectType, result);
	}
	ratio = Util::getTemporalConversionRatio(DT_SECOND, expectType);
	int result;
	if (ratio > 0) {
		val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ * 3600 % 86400 * ratio);
	} else {
		val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ * 3600 % 86400 / (-ratio));
	}
	return Util::createObject(expectType, result);
}

ConstantSP DateTime::castTemporal(DATA_TYPE expectType) {
	if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
		throw RuntimeException("castTemporal from DATETIME to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_DATETIME)
		return getValue();

	if (expectType == DT_DATEHOUR) {
		int result;

		int tail = static_cast<int>((val_ < 0) && ((val_ % 3600)) != 0);
		val_ == INT_MIN ? result = INT_MIN : result = val_ / 3600 - tail;
		return Util::createObject(expectType, result);
	}
	long long ratio = Util::getTemporalConversionRatio(DT_DATETIME, expectType);
	if (expectType == DT_NANOTIMESTAMP || expectType == DT_TIMESTAMP) {
		long long result;

		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio;
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_DATE) {
		int result;
		ratio = -ratio;

		int tail = static_cast<int>((val_ < 0) && ((val_ % ratio)) != 0);
		val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>((val_ / ratio) - tail);
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_MONTH) {
		int result;

		if (val_ == INT_MIN) {
			result = INT_MIN;
		} else {
			int year, month, day;
			Util::parseDate(val_ / 86400, year, month, day);
			result = year * 12 + month - 1;
		}
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_NANOTIME) {
		long long result;

		int remainder = val_ % 86400;
		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)(remainder + (static_cast<int>((val_ < 0) && (remainder != 0)) * 86400)) * 1000000000LL;
		return Util::createObject(expectType, result);
	}
	ratio = Util::getTemporalConversionRatio(DT_SECOND, expectType);
	int result;
	if (ratio > 0) {
		int remainder = val_ % 86400;
		val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>((remainder + static_cast<int>((val_ < 0) && (remainder != 0)) * 86400) * ratio);
	} else {
		ratio = -ratio;
		int remainder = val_ % 86400;
		val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>((remainder + static_cast<int>((val_ < 0) && (remainder != 0)) * 86400) / ratio);
	}
	return Util::createObject(expectType, result);
}

ConstantSP Minute::castTemporal(DATA_TYPE expectType) {
	if (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP) {
		throw RuntimeException("castTemporal from MINUTE to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType != DT_TIME && expectType != DT_NANOTIME && expectType != DT_SECOND && expectType != DT_MINUTE) {
		throw RuntimeException("castTemporal from MINUTE to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_MINUTE)
		return getValue();

	long long ratio = Util::getTemporalConversionRatio(DT_MINUTE, expectType);
	if (expectType == DT_NANOTIME) {
		long long result;

		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio;
		return Util::createObject(expectType, result);
	}
	int result;

	val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ * ratio);
	return Util::createObject(expectType, result);
}

ConstantSP Month::castTemporal(DATA_TYPE expectType) {
	if (expectType == DT_MONTH)
		return getValue();
	throw RuntimeException("castTemporal from MONTH to " + Util::getDataTypeString(expectType) + " not supported ");
}

ConstantSP Second::castTemporal(DATA_TYPE expectType) {
	if (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP) {
		throw RuntimeException("castTemporal from SECOND to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType != DT_TIME && expectType != DT_NANOTIME && expectType != DT_SECOND && expectType != DT_MINUTE) {
		throw RuntimeException("castTemporal from SECOND to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_SECOND)
		return getValue();

	long long ratio = Util::getTemporalConversionRatio(DT_SECOND, expectType);
	if (expectType == DT_NANOTIME) {
		long long result;

		val_ == INT_MIN ? result = LLONG_MIN : result = val_ * ratio;
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_TIME) {
		int result;

		val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ * ratio);
		return Util::createObject(expectType, result);
	}
	int result;

	val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ / (-ratio));
	return Util::createObject(expectType, result);
}
ConstantSP Time::castTemporal(DATA_TYPE expectType) {
	if (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP) {
		throw RuntimeException("castTemporal from TIME to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType != DT_TIME && expectType != DT_NANOTIME && expectType != DT_SECOND && expectType != DT_MINUTE) {
		throw RuntimeException("castTemporal from TIME to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_TIME)
		return getValue();

	long long ratio = Util::getTemporalConversionRatio(DT_TIME, expectType);
	if (expectType == DT_NANOTIME) {
		long long result;

		val_ == INT_MIN ? result = LLONG_MIN : result = (long long)val_ * ratio;
		return Util::createObject(expectType, result);
	}
	int result;

	val_ == INT_MIN ? result = INT_MIN : result = static_cast<int>(val_ / (-ratio));
	return Util::createObject(expectType, result);
}

ConstantSP NanoTime::castTemporal(DATA_TYPE expectType) {
	if (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP) {
		throw RuntimeException("castTemporal from NANOTIME to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType != DT_TIME && expectType != DT_NANOTIME && expectType != DT_SECOND && expectType != DT_MINUTE) {
		throw RuntimeException("castTemporal from NANOTIME to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_NANOTIME)
		return getValue();

	long long ratio = Util::getTemporalConversionRatio(DT_NANOTIME, expectType);

	int result;
	val_ == LLONG_MIN ? result = INT_MIN : result = static_cast<int>(val_ / (-ratio));
	return Util::createObject(expectType, result);
}

ConstantSP Timestamp::castTemporal(DATA_TYPE expectType) {
	if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
		throw RuntimeException("castTemporal from TIMESTAMP to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_TIMESTAMP)
		return getValue();

	if (expectType == DT_DATEHOUR) {
		int result;

		int tail = static_cast<int>((val_ < 0) && ((val_ % 3600000)) != 0);
		val_ == LLONG_MIN ? result = INT_MIN : result = static_cast<int>((val_ / 3600000LL) - tail);
		return Util::createObject(expectType, result);
	}
	long long ratio = Util::getTemporalConversionRatio(DT_TIMESTAMP, expectType);
	if (expectType == DT_NANOTIMESTAMP) {
		long long result;

		val_ == LLONG_MIN ? result = LLONG_MIN : result = val_ * ratio;
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_DATE || expectType == DT_DATETIME) {
		int result;
		ratio = -ratio;

		int tail = static_cast<int>((val_ < 0) && ((val_ % ratio)) != 0);
		val_ == LLONG_MIN ? result = INT_MIN : result = static_cast<int>((val_ / ratio) - tail);
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_MONTH) {
		int result;

		if (val_ == LLONG_MIN) {
			result = INT_MIN;
		} else {
			int year, month, day;
			Util::parseDate(static_cast<int>(val_ / 86400000), year, month, day);
			result = year * 12 + month - 1;
		}
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_NANOTIME) {
		long long result;

		int remainder = val_ % 86400000;
		val_ == LLONG_MIN ? result = LLONG_MIN : result = (remainder + static_cast<int>((val_ < 0) && (remainder != 0)) * 86400000) * 1000000LL;
		return Util::createObject(expectType, result);
	}
	ratio = Util::getTemporalConversionRatio(DT_TIME, expectType);
	int result;
	if (ratio < 0)
		ratio = -ratio;

	int remainder = val_ % 86400000;
	val_ == LLONG_MIN ? result = INT_MIN : result = static_cast<int>((remainder + static_cast<int>((val_ < 0) && (remainder != 0)) * 86400000) / ratio);
	return Util::createObject(expectType, result);
}

ConstantSP NanoTimestamp::castTemporal(DATA_TYPE expectType) {
	if (expectType != DT_DATEHOUR && (expectType < DT_DATE || expectType > DT_NANOTIMESTAMP)) {
		throw RuntimeException("castTemporal from NANOTIMESTAMP to " + Util::getDataTypeString(expectType) + " not supported ");
	}
	if (expectType == DT_NANOTIMESTAMP)
		return getValue();

	if (expectType == DT_DATEHOUR) {
		int result;

		int tail = static_cast<int>((val_ < 0) && ((val_ % 3600000000000LL)) != 0);
		val_ == LLONG_MIN ? result = INT_MIN : result = static_cast<int>((val_ / 3600000000000LL) - tail);
		return Util::createObject(expectType, result);
	}
	long long ratio = -Util::getTemporalConversionRatio(DT_NANOTIMESTAMP, expectType);
	if (expectType == DT_TIMESTAMP) {
		long long result;

		int tail = static_cast<int>((val_ < 0) && ((val_ % ratio)) != 0);
		val_ == LLONG_MIN ? result = LLONG_MIN : result = val_ / ratio - tail;
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_DATE || expectType == DT_DATETIME) {
		int result;

		int tail = static_cast<int>((val_ < 0) && ((val_ % ratio)) != 0);
		val_ == LLONG_MIN ? result = INT_MIN : result = static_cast<int>((val_ / ratio) - tail);
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_MONTH) {
		int result;

		if (val_ == LLONG_MIN) {
			result = INT_MIN;
		} else {
			int year, month, day;
			Util::parseDate(static_cast<int>(val_ / 86400000000000LL), year, month, day);
			result = year * 12 + month - 1;
		}
		return Util::createObject(expectType, result);
	}
	if (expectType == DT_NANOTIME) {
		long long result;

		long long remainder = val_ % 86400000000000LL;
		val_ == LLONG_MIN ? result = LLONG_MIN : result = (remainder + static_cast<long long>(val_ < 0 && (remainder != 0)) * 86400000000000LL);
		return Util::createObject(expectType, result);
	}
	ratio = Util::getTemporalConversionRatio(DT_NANOTIME, expectType);
	int result;
	ratio = -ratio;

	long long remainder = val_ % 86400000000000LL;
	val_ == LLONG_MIN ? result = INT_MIN : result = static_cast<int>((remainder + static_cast<long long>(val_ < 0 && (remainder != 0)) * 86400000000000LL) / ratio);
	return Util::createObject(expectType, result);
}

#define INSTANTIATE(class_type_t)                                                                           \
    template class AbstractScalar<class_type_t>;                                                            \
    template bool AbstractScalar<class_type_t>::getDecimal(INDEX, int, int, Decimal32::raw_data_t *) const; \
    template bool AbstractScalar<class_type_t>::getDecimal(INDEX, int, int, Decimal64::raw_data_t *) const; \
	template bool AbstractScalar<class_type_t>::getDecimal(INDEX, int, int, Decimal128::raw_data_t *) const;

INSTANTIATE(char)
INSTANTIATE(short)
INSTANTIATE(int)
INSTANTIATE(long long)
INSTANTIATE(float)
INSTANTIATE(double)

} // namespace dolphindb
