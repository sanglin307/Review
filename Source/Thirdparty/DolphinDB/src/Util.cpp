/*
 * Util.cpp
 *
 *  Created on: Sep 3, 2012
 *      Author: dzhou
 */

#include "Util.h"
#include "Constant.h"
#include "ConstantFactory.h"
#include "ConstantImp.h"
#include "DomainImp.h"
#include "ErrorCodeInfo.h"
#include "Exceptions.h"
#include "Guid.h"
#include "ScalarImp.h"
#include "SmartPointer.h"
#include "SymbolBase.h"
#include "Table.h"
#include "TableImp.h"
#include "Types.h"
#include "Vector.h"
#include "internal/WideInteger.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cfloat>
#include <chrono>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <utility>
#include <vector>

#ifdef MAC
#include <sys/syscall.h>
#endif

#ifdef __linux__
#include <pthread.h>
#include <unistd.h>
#define LIKE_TYPE(x, y) (__builtin_expect((x), y))
#else
#define LIKE_TYPE(x, y) (x)
#endif
namespace dolphindb{

#if __cplusplus < 201703L
constexpr int Util::BUF_SIZE;
#endif

using std::string;
using std::vector;

static SmartPointer<ConstantFactory> s_constFactory(new ConstantFactory());
const bool Util::LITTLE_ENDIAN_ORDER = isLittleEndian();
std::string Util::VER = "3.00.3.0";
int Util::VERNUM = 30030;
std::string Util::BUILD = "2025.07.02";

int Util::SEQUENCE_SEARCH_NUM_THRESHOLD = 10;
int Util::MAX_LENGTH_FOR_ANY_VECTOR = 1048576;
double Util::SEQUENCE_SEARCH_RATIO_THRESHOLD = 200000;
int Util::DISPLAY_ROWS = 30;
int Util::DISPLAY_COLS = 10;
int Util::DISPLAY_WIDTH = 120;
int Util::CONST_VECTOR_MAX_SIZE = 128;
int Util::cumMonthDays[13] = {0,31,59,90,120,151,181,212,243,273,304,334,365};
int Util::cumLeapMonthDays[13] = {0,31,60,91,121,152,182,213,244,274,305,335,366};
int Util::monthDays[12] = {31,28,31,30,31,30,31,31,30,31,30,31};
int Util::leapMonthDays[12] = {31,29,31,30,31,30,31,31,30,31,30,31};
char Util::escapes[128] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
		0,0,'"',0,0,0,0,'\'',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
		0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,'\\',0,0,0,
		0,0,0,0,0,0,0,0,0,0,0,0,0,0,'\n',0,0,0,'\r',0,'\t',0,0,0,0,0,0,0,0,0,0,0};
std::string Util::duSyms[10] = {"ns", "us", "ms", "s", "m", "h", "d", "w", "M", "y"};
long long Util::tmporalUplimit[9] = {0, 0, 86400000, 1440, 86400, 0, 0, 86400000000000LL, 0};
long long Util::tmporalDurationRatioMatrix[9][10] = {
		{0, 0, 0, 0, 0, 0, 1, 7, 0, 0}, //DATE
		{0, 0, 0, 0, 0, 0, 0, 0, 1, 0}, //MONTH
		{0, 0, 1, 1000, 60000, 3600000, 0, 0, 0, 0}, //TIME
		{0, 0, 0, 0, 1, 60, 0, 0, 0, 0}, //Minute
		{0, 0, 0, 1, 60, 3600, 0, 0, 0, 0}, //SECOND
		{0, 0, 0, 1, 60, 3600, 86400, 604800, 0, 0}, //DATETIME
		{0, 0, 1, 1000, 60000, 3600000, 86400000LL, 604800000LL, 0, 0}, //TIMESTAMP
		{1, 1000, 1000000, 1000000000, 60000000000LL, 3600000000000LL, 0, 0, 0, 0}, //NANOTIME
		{1, 1000, 1000000, 1000000000, 60000000000LL, 3600000000000LL, 86400000000000LL, 604800000000000LL, 0, 0} //NANOTIMESTAMP
};
long long Util::tmporalRatioMatrix[81] = {
		1, 0, 0, 0, 0, 86400, 86400000, 0, 86400000000000LL,
		0, 0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 1, -60000, -1000, 0, 0, 1000000, 0,
		0, 0, 60000, 1, 60, 0, 0, 60000000000LL, 0,
		0, 0, 1000, -60, 1, 0, 0, 1000000000LL, 0,
		-86400, 0, 0, 0, 0, 1, 1000, 0, 1000000000LL,
		-86400000, 0, 0, 0, 0, -1000, 1, 0, 1000000LL,
		0, 0, -1000000, -60000000000LL, -1000000000, 0, 0, 1, 0,
		-86400000000000LL, 0, 0, 0, 0, -1000000000, -1000000, 0, 1
};

char Util::escape(char original) {
	return Util::escapes[(int)original];
}

int Util::countDays(int year, int month, int day) {
	if(month<1 || month>12 || day<0)
		return INT_MIN;
    int divide400Years = year / 400;
    int offset400Years = year % 400;
    int days = (divide400Years * 146097) + (offset400Years * 365) - 719529;
    if(offset400Years != 0) days += (offset400Years - 1) / 4 + 1 - (offset400Years - 1) / 100;
    if((year%4==0 && year%100!=0) || year%400==0){
		days+=cumLeapMonthDays[month-1];
		return day <= leapMonthDays[month - 1] ? days + day : INT_MIN;
	}
	days+=cumMonthDays[month-1];
	return day <= monthDays[month - 1] ? days + day : INT_MIN;
}

int Util::parseYear(int days) {
    days += 719529;
    int circleIn400Years = days / 146097;
    int offsetIn400Years = days % 146097;
    int resultYear = circleIn400Years * 400;
    int similarYears = offsetIn400Years / 365;
    int tmpDays = similarYears * 365;
    if(similarYears != 0) tmpDays += (similarYears - 1) / 4 + 1 - (similarYears - 1) / 100;
    if(tmpDays >= offsetIn400Years) similarYears --;

    return similarYears + resultYear;
}

void Util::parseDate(int days, int& year, int& month, int& day)
{
    constexpr int offsetAD {719529}; // convert 1970 to 0000
    constexpr int daysInCycle {146097}; // days in 400 years
    constexpr int cycleYears {400};
    constexpr int daysOfYear {365};
    int64_t value {days};
    value += offsetAD;
    int cycles = static_cast<int>(value / daysInCycle);
    int fraction = static_cast<int>(value % daysInCycle);
    int estimate = static_cast<int>(fraction / daysOfYear);
    int tmpDays = estimate * daysOfYear;
    if (estimate > 0) {
        tmpDays += (estimate + 3) / 4 - (estimate - 1) / 100;
    }
    if (tmpDays >= fraction) {
        --estimate;
    }
    year = cycles * cycleYears + estimate;
    value -= cycles * daysInCycle + tmpDays;
    days = static_cast<int>(value);
    bool leap = ( (year%4==0 && year%100!=0) || year%400==0 );
    if(days <= 0) {
        days += leap ? 366 : 365;
    }
    month = days / 32+1;
    int *cumDays = leap ? cumLeapMonthDays : cumMonthDays;
    if(days > cumDays[month]) {
        month++;
    }
    day = days - cumDays[month - 1];
}

int Util::getMonthEnd(int days){
    int year, month, day;
    parseDate(days, year, month, day);
    bool leapYear = ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0));
    int daysInMonth = leapYear ? leapMonthDays[month - 1] : monthDays[month - 1];
    return days + daysInMonth - day;
}

int Util::getMonthStart(int days){
    int year, month, day;
    parseDate(days, year, month, day);
    return days + 1 - day;
}

char* Util::allocateMemory(INDEX size, bool throwIfFail){
	try{
		return new char[size];
	}
	catch(...){
		if(throwIfFail)
			throw MemoryException();
		return nullptr;
	}
}

Constant* Util::parseConstant(int type, const std::string & word){
	if(type < 0)
		return nullptr;
	return s_constFactory->parseConstant(type,word);
}

Constant* Util::createConstant(DATA_TYPE type, int extraParam){
	return s_constFactory->createConstant(type, extraParam);
}

Constant* Util::createNullConstant(DATA_TYPE dataType, int extraParam){
	Constant* result=s_constFactory->createConstant(dataType, extraParam);
	result->setNull();
	return result;
}

Constant* Util::createBool(bool val)
{
    return new Bool(val);
}

Constant* Util::createBool(char val){
	return new Bool(val);
}

Constant* Util::createChar(char val){
	return new Char(val);
}

Constant* Util::createShort(short val){
	return new Short(val);
}

Constant* Util::createInt(int val){
	return new Int(val);
}

Constant* Util::createLong(long long val) {
	return new Long(val);
}

Constant* Util::createFloat(float val) {
	return new Float(val);
}

Constant* Util::createDouble(double val){
	return new Double(val);
}

Constant* Util::createString(const std::string & val){
	return new String(val);
}

Constant* Util::createBlob(const std::string & val) {
	return new String(val,true);
}

Constant* Util::createDate(int year, int month, int day){
	return new Date(year, month, day);
}

Constant* Util::createDate(int days){
	return new Date(days);
}

Constant* Util::createMonth(int year, int month){
	return new Month(year, month);
}

Constant* Util::createMonth(int months){
	return new Month(months);
}

Constant* Util::createNanoTime(int hour, int minute, int second, int nanosecond){
	return new NanoTime(hour, minute, second, nanosecond);
}

Constant* Util::createNanoTime(long long nanoseconds){
	return new NanoTime(nanoseconds);
}

Constant* Util::createTime(int hour, int minute, int second, int millisecond){
	return new Time(hour, minute, second, millisecond);
}

Constant* Util::createTime(int milliseconds){
	return new Time(milliseconds);
}

Constant* Util::createSecond(int hour, int minute, int second){
	return new Second(hour, minute, second);
}

Constant* Util::createSecond(int seconds){
	return new Second(seconds);
}

Constant* Util::createMinute(int hour, int minute){
	return new Minute(hour, minute);
}

Constant* Util::createMinute(int minutes){
	return new Minute(minutes);
}

Constant* Util::createNanoTimestamp(int year, int month, int day, int hour, int minute, int second, int nanosecond){
	return new NanoTimestamp(year, month, day, hour, minute, second, nanosecond);
}

Constant* Util::createNanoTimestamp(long long nanoseconds){
	return new NanoTimestamp(nanoseconds);
}

Constant* Util::createTimestamp(int year, int month, int day, int hour, int minute, int second, int millisecond){
	return new Timestamp(year, month, day, hour, minute, second, millisecond);
}

Constant* Util::createTimestamp(long long milliseconds){
	return new Timestamp(milliseconds);
}

Constant* Util::createDateTime(int year, int month, int day, int hour, int minute, int second){
	return new DateTime(year, month, day, hour, minute, second);
}

Constant* Util::createDateTime(int seconds){
	return new DateTime(seconds);
}

Constant * Util::createDateHour(int hours) {
    return new DateHour(hours);
}

Constant * Util::createDateHour(int year, int month, int day, int hour) {
    return new DateHour(year, month, day, hour);
}

Constant* Util::createDecimal32(int scale, double value) {
	auto *decimal=new Decimal32(scale);
	decimal->setDouble(value);
	return decimal;
}

Constant* Util::createDecimal64(int scale, double value) {
	auto *decimal=new Decimal64(scale);
	decimal->setDouble(value);
	return decimal;
}

Constant* Util::createDecimal128(int scale, double value) {
	auto *decimal=new Decimal128(scale);
	decimal->setDouble(value);
	return decimal;
}

bool Util::isFlatDictionary(Dictionary* dict){
	if(dict->getKeyCategory()!=LITERAL || dict->size()>1024)
		return false;
	int numCol=dict->size();
	ConstantSP keys=dict->keys();
	ConstantSP value;
	for(int i=0;i<numCol;++i){
		value=dict->getMember(keys->get(i));
		if(value->isNull() || !value->isScalar())
			return false;
		DATA_TYPE type=value->getType();
		if(type==DT_VOID || type>DT_STRING)
			return false;
	}
	return true;
}

Table* Util::createTable(Dictionary* dict, int size){
	if(dict->getKeyCategory()!=LITERAL || dict->size()>1024)
		return nullptr;
	int numCol=dict->size();
	ConstantSP keys=dict->keys();
	ConstantSP value;
	vector<ConstantSP> cols;
	vector<string> names;
	Vector *pVec;
	for(int i=0;i<numCol;++i){
		names.push_back(keys->getStringRef(i));
		value=dict->getMember(keys->get(i));
		if(value->isNull() || !value->isScalar())
			throw RuntimeException("Invalid column format " + names.back());
		DATA_TYPE type=value->getType();
		if (type >= ARRAY_TYPE_BASE) {
			pVec = createArrayVector(type, (INDEX)size, 0, true, value->getExtraParamForType());
		} else {
			if (type == DT_VOID || type == DT_OBJECT || type == DT_ANY)
				throw RuntimeException("Invalid column type " + getDataTypeString(type));
			pVec = createVector(type, (INDEX)size, 0, true, value->getExtraParamForType());
		}
		cols.emplace_back(pVec);
	}
	vector<int> tableKey;
	return new BasicTable(cols,names,tableKey);
}

Table* Util::createTable(const vector<string>& colNames, const vector<DATA_TYPE>& colTypes, INDEX size, INDEX capacity, const vector<int>& extraParams){
	vector<ConstantSP> cols;
	size_t numCol = colNames.size();
	for(size_t i=0;i<numCol;++i){
		int extraParam = 0;
		if (i < extraParams.size())
			extraParam = extraParams[i];
		DATA_TYPE type = colTypes[i];
		Vector *pVec;
		if (type >= ARRAY_TYPE_BASE) {
			pVec = createArrayVector(type, size, capacity,true, extraParam);
		} else {
			if (type == DT_VOID || type == DT_OBJECT || type == DT_ANY) {
				throw RuntimeException("Invalid column type "+getDataTypeString(type));
			}
			pVec = createVector(type, size, capacity, true, extraParam);
		}
		cols.emplace_back(pVec);
	}
	vector<int> tableKey;
	return new BasicTable(cols, colNames, tableKey);
}

Table* Util::createTable(const vector<string>& colNames, const vector<ConstantSP>& cols){
	vector<int> tableKey;
	return new BasicTable(cols, colNames, tableKey);
}

Dictionary* Util::createDictionary(DATA_TYPE keyType, DATA_TYPE valueType){
	return s_constFactory->createDictionary(convertToIntegralDataType(keyType),keyType,valueType);
}

Set* Util::createSet(DATA_TYPE keyType, INDEX capacity){
	return s_constFactory->createSet(convertToIntegralDataType(keyType),keyType,capacity);
}

Vector* Util::createVector(DATA_TYPE type, INDEX size, INDEX capacity, bool fast, int extraParam, void* data, bool containNull){
	if (type < ARRAY_TYPE_BASE)
		return s_constFactory->createConstantVector(type, size, capacity, true, extraParam, data, nullptr, 0, containNull);
	return createArrayVector(type, size, capacity, fast, extraParam);
}

Vector* Util::createArrayVector(DATA_TYPE type, INDEX size, INDEX capacity, bool fast, int extraParam, void* data, INDEX *pindex, bool containNull){
	std::ignore = fast;
	return s_constFactory->createConstantArrayVector(type,size,capacity,true,extraParam, data, pindex, nullptr, 0, containNull);
}

Vector* Util::createArrayVector(const VectorSP& index, const VectorSP& value) {
	if (!index->isSorted(true)) {
		throw RuntimeException("Failed to create an array vector, index must be incremental.");
	}
	if (index->getIndex(index->size() - 1) != value->size()) {
		throw RuntimeException("Failed to create an array vector, the size of value is inconsistent.");
	}
	return new FastArrayVector(index, value);
}

Vector* Util::createMatrix(DATA_TYPE type, int cols, int rows, int colCapacity,int extraParam, void* data, bool containNull){
	return s_constFactory->createConstantMatrix(type, cols, rows, colCapacity, extraParam, data, nullptr, 0, containNull);
}

Vector* Util::createDoubleMatrix(int cols, int rows){
	return s_constFactory->createConstantMatrix(DT_DOUBLE, cols, rows, 0, 0, nullptr, nullptr, 0, false);
}

Vector* Util::createIndexVector(INDEX start, INDEX length){
	Vector* index = Util::createVector(DT_INDEX, length);
	if(index == nullptr)
		throw RuntimeException("Failed to create an index vector.");

	INDEX* indices = index->getIndexArray();
	for(INDEX i=0;i<length;++i) indices[i]=start+i;

	return index;
}

Vector* Util::createIndexVector(INDEX length, bool arrayOnly, INDEX capacity){
	std::ignore = arrayOnly;
	capacity = std::max(capacity, length);
	auto* indices = new INDEX[capacity];
#ifdef INDEX64
	return new FastLongVector(length, capacity,indices,false);
#else
	return new FastIntVector(length, capacity,indices,false);
#endif
}

void Util::toHex(const unsigned char* data, size_t len, bool littleEndian, char* str){
	size_t lastPos = (len*2) - 1;
	for(size_t i=0; i<len; ++i){
		unsigned char ch = data[i];
		char low4 = ch & (unsigned char)15;
		char high4 =  ch>>4;
		if(littleEndian){
			str[lastPos - (2*i)] = low4 >= 10 ? 87 + low4 : 48 + low4;
			str[lastPos - 1 - (2*i)] = high4 >= 10 ? 87 + high4 : 48 + high4;
		} else{
			str[(2*i) + 1] = low4 >= 10 ? 87 + low4 : 48 + low4;
			str[2*i] = high4 >= 10 ? 87 + high4 : 48 + high4;
		}
	}
}

bool Util::fromHex(const char* str, size_t len, bool littleEndian, unsigned char* data){
	size_t lastPos = (len/2) - 1;
	for(size_t i=0; i<len; i += 2){
		char high = str[i];
		char low = str[i+1];
		int highValue = high >= 97 ? high -87 : (high >= 65 ? high -55 : (high >= 58 ? -1 : high -48));
		int lowValue = low >= 97 ? low -87 : (low >= 65 ? low -55 : (low >= 58 ? -1 : low -48));
		if(highValue < 0 || highValue > 15 || lowValue < 0 || lowValue > 15)
			return false;
		data[littleEndian ? (lastPos - (i/2)) : i/2] = static_cast<unsigned char>(((unsigned)highValue << 4) + lowValue);
	}
	return true;
}

inline void charToHexPair(unsigned char ch, char* buf) {
	char low  = ch & 15;
	char high = ch >> 4;
	buf[0] = high<10 ? high + 48 : high + 87;
	buf[1] = low<10 ? low + 48 : low + 87;
}

void Util::toGuid(const unsigned char* data, char* str) {
	str[8] = '-';
	str[13] = '-';
	str[18] = '-';
	str[23] = '-';
#ifndef BIGENDIANNESS
	for(int i=0; i<4; ++i)
		charToHexPair(data[15 - i], str + (2*i));
	charToHexPair(data[11], str + 9);
	charToHexPair(data[10], str + 11);
	charToHexPair(data[9], str + 14);
	charToHexPair(data[8], str + 16);
	charToHexPair(data[7], str + 19);
	charToHexPair(data[6], str + 21);
	for(int i=5; i>=0; --i)
		charToHexPair(data[i], str + 34 - (2*i));
#else
	for(int i=0; i<4; ++i)
		charToHexPair(data[i], str + 2*i);
	charToHexPair(data[4], str + 9);
	charToHexPair(data[5], str + 11);
	charToHexPair(data[6], str + 14);
	charToHexPair(data[7], str + 16);
	charToHexPair(data[8], str + 19);
	charToHexPair(data[9], str + 21);
	for(int i=10; i<16; ++i)
		charToHexPair(data[i], str + 4 + 2*i);
#endif
}

inline unsigned char hexPairToChar(char a) {
	if (islower(a)) {
		return a - 'a' + 10;
	}
	if (isupper(a)) {
		return a - 'A' + 10;
	}
	return a - '0';
}

inline unsigned char hexPairToChar(char a, char b) {

	return (hexPairToChar(a) << 4) + hexPairToChar(b);
}

bool Util::fromGuid(const char* str, unsigned char* data){
	if(str[8] != '-' || str[13] != '-' || str[18] != '-' || str[23] != '-')
		return false;
#ifndef BIGENDIANNESS
	for(int i=0; i<4; ++i)
		data[15 - i] = hexPairToChar(str[2*i], str[(2*i)+1]);
	data[11] = hexPairToChar(str[9], str[10]);
	data[10] = hexPairToChar(str[11], str[12]);
	data[9] = hexPairToChar(str[14], str[15]);
	data[8] = hexPairToChar(str[16], str[17]);
	data[7] = hexPairToChar(str[19], str[20]);
	data[6] = hexPairToChar(str[21], str[22]);

	for(int i=10; i<16; ++i)
		data[15 - i] = hexPairToChar(str[(2*i)+4], str[(2*i)+5]);
#else
	for(int i=0; i<4; ++i)
		data[i] = hexPairToChar(str[2*i], str[2*i+1]);
	data[4] = hexPairToChar(str[9], str[10]);
	data[5] = hexPairToChar(str[11], str[12]);
	data[6] = hexPairToChar(str[14], str[15]);
	data[7] = hexPairToChar(str[16], str[17]);
	data[8] = hexPairToChar(str[19], str[20]);
	data[9] = hexPairToChar(str[21], str[22]);

	for(int i=10; i<16; ++i)
		data[i] = hexPairToChar(str[2*i+4], str[2*i+5]);
#endif
	return true;
}

DATA_TYPE Util::getDataType(const std::string & typestr){
	return s_constFactory->getDataType(lower(typestr));
}

DATA_FORM Util::getDataForm(const std::string & formstr){
	return s_constFactory->getDataForm(lower(formstr));
}

char Util::getDataTypeSymbol(DATA_TYPE type){
	return s_constFactory->getDataTypeSymbol(type);
}

std::string Util::getDataTypeString(DATA_TYPE type){
	return s_constFactory->getDataTypeString(type);
}

std::string Util::getDataFormString(DATA_FORM form){
	return s_constFactory->getDataFormString(form);
}

std::string Util::getTableTypeString(TABLE_TYPE type){
	return s_constFactory->getTableTypeString(type);
}

DATA_TYPE Util::getDataType(char ch){
	if(ch=='b')
		return DT_BOOL;
	if(ch=='c')
		return DT_CHAR;
	if(ch=='h')
		return DT_SHORT;
	if(ch=='i')
		return DT_INT;
	if(ch=='l')
		return DT_LONG;
	if(ch=='f')
		return DT_FLOAT;
	if(ch=='F')
		return DT_DOUBLE;
	if(ch=='d')
		return DT_DATE;
	if(ch=='M')
		return DT_MONTH;
	if(ch=='m')
		return DT_MINUTE;
	if(ch=='s')
		return DT_SECOND;
	if(ch=='t')
		return DT_TIME;
	if(ch=='D')
		return DT_DATETIME;
	if(ch=='T')
		return DT_TIMESTAMP;
	if(ch=='n')
		return DT_NANOTIME;
	if(ch=='N')
		return DT_NANOTIMESTAMP;
	if(ch=='S')
		return DT_SYMBOL;
	if(ch=='W')
		return DT_STRING;
	return DT_VOID;
}

DURATION_UNIT Util::getDurationUnit(const std::string & typestr){
	for(int i=0; i<10; ++i){
		if(duSyms[i] == typestr)
			return (DURATION_UNIT)i;
	}
	return (DURATION_UNIT)-1;
}

long long Util::getTemporalDurationConversionRatio(DATA_TYPE t, DURATION_UNIT du){
	return tmporalDurationRatioMatrix[t - DT_DATE][du];
}

long long Util::getTemporalUplimit(DATA_TYPE t){
	return tmporalUplimit[t - DT_DATE];
}

DATA_TYPE Util::convertToIntegralDataType(DATA_TYPE type){
	if(type==DT_TIME || type==DT_SECOND || type==DT_MINUTE || type==DT_DATE
			|| type==DT_DATETIME || type==DT_MONTH || type==DT_DATEHOUR || type==DT_DATEMINUTE)
		return DT_INT;
	if(type==DT_TIMESTAMP || type==DT_NANOTIME || type==DT_NANOTIMESTAMP)
		return DT_LONG;
	if(type==DT_UUID || type==DT_IP)
		return DT_INT128;
	return type;
}

long long Util::getTemporalConversionRatio(DATA_TYPE first, DATA_TYPE second){
	return tmporalRatioMatrix[((first - DT_DATE)*9) + (second - DT_DATE)];
}

DATA_CATEGORY Util::getCategory(DATA_TYPE type){
	if (type == DT_TIME || type == DT_SECOND || type == DT_MINUTE || type == DT_DATE || type == DT_DATEHOUR || type == DT_DATEMINUTE
		|| type == DT_DATETIME || type == DT_MONTH || type == DT_NANOTIME || type == DT_NANOTIMESTAMP || type == DT_TIMESTAMP)
		return TEMPORAL;
	if (type == DT_INT || type == DT_LONG || type == DT_SHORT || type == DT_CHAR)
		return INTEGRAL;
	if (type == DT_BOOL)
		return LOGICAL;
	if (type == DT_DOUBLE || type == DT_FLOAT)
		return FLOATING;
	if (type == DT_STRING || type == DT_SYMBOL || type == DT_BLOB)
		return LITERAL;
	if (type == DT_INT128 || type == DT_UUID || type == DT_IP)
		return BINARY;
	if (type == DT_ANY)
		return MIXED;
	if (type == DT_VOID)
		return NOTHING;
	if (type == DT_DECIMAL32 || type == DT_DECIMAL64 || type == DT_DECIMAL128)
		return DENARY;
	if(type >= ARRAY_TYPE_BASE)
		return ARRAY;
	return SYSTEM;
}

bool Util::equalIgnoreCase(const std::string & str1, const std::string & str2){
	std::size_t len=str1.size();
	if(len!=str2.size())
		return false;
	std::size_t i;
	for(i=0;i<len && toLower(str1[i])==toLower(str2[i]);i++);
	if(i>=len)
		return true;
	return false;
}

std::string Util::ltrim(const std::string & str){
	const char* begin=str.c_str();
	while(*begin==' ') ++begin;
	return begin;
}

std::string Util::trim(const std::string & str){
	const char* begin=str.c_str();
	while(*begin==' ') ++begin;

	const char* end=begin;
	const char* cur=begin;
	while(*cur != 0){
		if(*cur != ' ')
			end = cur;
		++cur;
	}

	return str.substr(begin - str.c_str(), end - begin + 1);
}

std::string Util::strip(const std::string & str){
	const char* begin=str.c_str();
	while(*begin==' ' || *begin=='\t' || *begin=='\r' || *begin=='\n') ++begin;

	const char* end=begin;
	const char* cur=begin;
	while(*cur != 0){
		if(*cur != ' ' && *cur!='\t' && *cur!='\r' && *cur!='\n')
			end = cur;
		++cur;
	}

	return str.substr(begin - str.c_str(), end - begin + 1);
}

int Util::wc(const char* str){
	int count = 0;
	bool whiteSpace = true;
	while(*str != 0){
		if((*str>='0' && *str<='9') || (*str>='a' && *str<='z') || (*str>='A' && *str<='Z')){
			whiteSpace = false;
		} else if(!whiteSpace){
			++count;
			whiteSpace = true;
		}
		++str;
	}
	if(!whiteSpace)
		++count;
	return count;
}

std::string Util::replace(const std::string & str, const std::string & pattern, const std::string & replacement){
	string target;
	size_t start = 0;
	size_t len = pattern.size();
	while(true){
		size_t end = str.find(pattern, start);
		if(end == string::npos){
			target.append(str.substr(start));
			return target;
		}
		if (end > start)
			target.append(str.substr(start, end - start));
		target.append(replacement);
		start = end + len;
	}
	return target;
}

std::string Util::replace(const std::string & str, char pattern, char replacement){
	size_t len = str.size();
	string target(str);
	for(size_t i=0; i<len; ++i){
		if(str[i] == pattern)
			target[i] = replacement;
	}
	return target;
}

std::string Util::lower(const std::string & str){
	string data(str);
	std::transform(data.begin(), data.end(), data.begin(), [](char c){ return static_cast<char>(std::tolower(c)); });
	return data;
}

std::string Util::upper(const std::string & str){
	string data(str);
	std::transform(data.begin(), data.end(), data.begin(), [](char c){ return static_cast<char>(std::toupper(c)); });
	return data;
}

char Util::toUpper(char ch){
	return (ch>='a' && ch<='z') ? ch -32 : ch;
}

char Util::toLower(char ch){
	return (ch>='A' && ch<='Z') ? ch + 32 : ch;
}

std::string Util::convert(int val){
	char buf[15];
#ifdef _MSC_VER
	sprintf_s(buf, 15, "%d", val);
#else
	sprintf(buf,"%d",val);
#endif
	return std::string(buf);
}

std::string Util::longToString(long long val){
	char buf[30];
#ifdef _MSC_VER
	sprintf_s(buf, 30, "%lld", val);
#else
	std::sprintf(buf,"%lld",val);
#endif
	return std::string(buf);
}

std::string Util::doubleToString(double val){
	char buf[30];
#ifdef _MSC_VER
	sprintf_s(buf, 30, "%f", val);
#else
	sprintf(buf, "%f", val);
#endif
	//trim unnecessary zeros after decimal point.
	char* cur=buf;
	while(*cur!='.') ++cur;
	char* end=cur-1;
	++cur;
	while(*cur!=0){
		if(*cur!='0')
			end=cur;
		++cur;
	}
	*(end+1)=0;
	return std::string(buf);
}

bool Util::endWith(const std::string & str, const std::string & end){
	if(end.empty() || str.size()<end.size())
		return false;
	return str.substr(str.size()-end.size(),end.size())==end;
}

bool Util::startWith(const std::string & str, const std::string & start){
	if(start.empty() || str.size()<start.size())
			return false;
	return str.substr(0,start.size())==start;
}

bool Util::isVariableCandidate(const std::string & word){
	char cur=word.at(0);
	if((cur<'a' || cur>'z') && (cur<'A' || cur>'Z'))
		return false;
	for(unsigned int i=1;i<word.length();i++){
		cur=word.at(i);
		if((cur<'a' || cur>'z') && (cur<'A' || cur>'Z') && (cur<'0' || cur>'9') && cur!='_')
			return false;
	}
	return true;
}

std::string Util::literalConstant(const std::string & str){
	string script(1,'"');
	size_t end = str.find('"');
	if(end == string::npos)
		script.append(str);
	else{
		size_t start = 0;
		while(end != string::npos){
			script.append(str.substr(start, end - start));
			script.append("\\\"");
			start = end + 1;
			end = str.find('"', start);
		}
		if(start < str.length())
			script.append(str.substr(start));
	}
	script.append(1,'"');
	return script;
}

long long Util::getNanoBenchmark(){
	return  std::chrono::steady_clock::now().time_since_epoch()/std::chrono::nanoseconds(1);
}

long long Util::getNanoEpochTime(){
	return  std::chrono::system_clock::now().time_since_epoch()/std::chrono::nanoseconds(1);
}

long long Util::getEpochTime(){
	return  std::chrono::system_clock::now().time_since_epoch()/std::chrono::milliseconds(1);
}

bool Util::getLocalTime(struct tm& result){
	time_t t;
	time(&t);
	return getLocalTime(t, result);
}

int Util::toLocalDateTime(int epochTime){
	time_t t = epochTime;
	struct tm lt;
	getLocalTime(t, lt);
    int days = countDays(lt.tm_year+1900,lt.tm_mon+1,lt.tm_mday);
    return  days == INT_MIN ? INT_MIN : (days * 86400) + ((lt.tm_hour * 60 + lt.tm_min)* 60 + lt.tm_sec);
}

int* Util::toLocalDateTime(int* epochTimes, int n){
	struct tm lt;
	for(int i=0; i<n; ++i){
		if(epochTimes[i] == INT_MIN)
			continue;
		time_t t = epochTimes[i];
		getLocalTime(t, lt);
	    int days = countDays(lt.tm_year+1900,lt.tm_mon+1,lt.tm_mday);
	    epochTimes[i] = days == INT_MIN ? INT_MIN : (days * 86400) + ((lt.tm_hour * 60 + lt.tm_min)* 60 + lt.tm_sec);
	}
	return epochTimes;
}

long long Util::toLocalTimestamp(long long epochTime){
	time_t t = epochTime / 1000;
	struct tm lt;
	getLocalTime(t, lt);
    int days = countDays(lt.tm_year+1900,lt.tm_mon+1,lt.tm_mday);
    return days == INT_MIN ? LLONG_MIN : (days * 86400000LL) + (((lt.tm_hour * 60 + lt.tm_min)* 60 + lt.tm_sec) * 1000LL) + (epochTime % 1000);
}

long long* Util::toLocalTimestamp(long long* epochTimes, int n){
	struct tm lt;
	for(int i=0; i<n; ++i){
		if(epochTimes[i] == LLONG_MIN)
			continue;
		time_t t = epochTimes[i] / 1000;
		getLocalTime(t, lt);
	    int days = countDays(lt.tm_year+1900,lt.tm_mon+1,lt.tm_mday);
	    epochTimes[i] = days == INT_MIN ? LLONG_MIN : (days * 86400000LL) + (((lt.tm_hour * 60 + lt.tm_min)* 60 + lt.tm_sec) * 1000LL) + (epochTimes[i] % 1000);
	}
	return epochTimes;
}

long long Util::toLocalNanoTimestamp(long long epochNanoTime){
	time_t t = epochNanoTime / 1000000000;
	struct tm lt;
	getLocalTime(t, lt);
    int days = countDays(lt.tm_year+1900,lt.tm_mon+1,lt.tm_mday);
    return days == INT_MIN ? LLONG_MIN : (days * 86400000000000LL) + (((lt.tm_hour * 60 + lt.tm_min)* 60 + lt.tm_sec) * 1000000000LL) + (epochNanoTime % 1000000000);
}

long long* Util::toLocalNanoTimestamp(long long* epochNanoTimes, int n){
	struct tm lt;
	for(int i=0; i<n; ++i){
		if(epochNanoTimes[i] == LLONG_MIN)
			continue;
		time_t t = epochNanoTimes[i] / 1000000000;
		getLocalTime(t, lt);
	    int days = countDays(lt.tm_year+1900,lt.tm_mon+1,lt.tm_mday);
	    epochNanoTimes[i] = days == INT_MIN ? LLONG_MIN : (days * 86400000000000LL) + (((lt.tm_hour * 60 + lt.tm_min)* 60 + lt.tm_sec) * 1000000000LL) + (epochNanoTimes[i] % 1000000000);
	}
	return epochNanoTimes;
}

std::string Util::toMicroTimestampStr(std::chrono::system_clock::time_point& tp, bool printDate){
	struct tm lt;
	std::time_t now_c = std::chrono::system_clock::to_time_t(tp);
	getLocalTime(now_c, lt);
	int microsecond= (tp.time_since_epoch()/std::chrono::microseconds(1)) % 1000000;
	char buf[100]{};
#ifdef _MSC_VER
	if(printDate)
		sprintf_s(buf, 100, "%04d-%02d-%02d %02d:%02d:%02d.%06d", lt.tm_year+1900, lt.tm_mon+1, lt.tm_mday, lt.tm_hour, lt.tm_min, lt.tm_sec, microsecond);
	else
		sprintf_s(buf, 100, "%02d:%02d:%02d.%06d",lt.tm_hour, lt.tm_min, lt.tm_sec, microsecond);
#else
	if(printDate)
		sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d.%06d", lt.tm_year+1900, lt.tm_mon+1, lt.tm_mday, lt.tm_hour, lt.tm_min, lt.tm_sec, microsecond);
	else
		sprintf(buf, "%02d:%02d:%02d.%06d",lt.tm_hour, lt.tm_min, lt.tm_sec, microsecond);
#endif
	return std::string(buf);
}

int Util::getDataTypeSize(DATA_TYPE type){
	if (type >= ARRAY_TYPE_BASE) {
		type = DATA_TYPE(type - ARRAY_TYPE_BASE);
	}
	if(type == DT_VOID || type == DT_BOOL || type == DT_CHAR || type == DT_COMPRESS){
		return sizeof(char);
	}
	if(type == DT_INT || type == DT_SYMBOL || type == DT_SECOND ||
			type == DT_DATE || type == DT_MONTH || type == DT_TIME ||
			type == DT_MINUTE || type == DT_DATETIME|| type==DT_DATEHOUR){
		return sizeof(int);
	}
	if(type == DT_SHORT){
		return sizeof(short);
	}
	if(type == DT_LONG || type == DT_TIMESTAMP || type == DT_NANOTIMESTAMP || type == DT_NANOTIME){
		return sizeof(long long);
	}
	if(type == DT_DOUBLE){
		return sizeof(double);
	}
	if(type == DT_FLOAT){
		return sizeof(float);
	}
	if (type == DT_DECIMAL32) {
		return sizeof(int32_t);
	}
	if (type == DT_DECIMAL64) {
		return sizeof(int64_t);
	}
	if(type == DT_DECIMAL128){
		return sizeof(wide_integer::int128);
	}
	if(type == DT_INT128||type== DT_IP||type==DT_UUID)
		return sizeof(Guid);
	return -1;
}

void Util::split(const char* s, char delim, vector<string> &elems){
	const char* start=s;
	int length=0;
	while(*s!=0){
		if(*s==delim){
			elems.emplace_back(start,length);
			++s;
			start=s;
			length=0;
		} else{
			++s;
			++length;
		}
	}
	if(*start!=0)
		elems.emplace_back(start,length);
}

vector<string> Util::split(const string &s, char delim) {
    vector<string> elems;
    split(s.c_str(), delim, elems);
    return elems;
}

bool Util::strWildCmp(const char* str, const char* pat) {
   const char* s;
   const char* p;
   bool star = false;

loopStart:
   for (s = str, p = pat; *s != 0; ++s, ++p) {
      switch (*p) {
         case '?':
            break;
         case '%':
            star = true;
            str = s, pat = p;
            if (*++pat == 0) return true;
            goto loopStart;
         default:
            if (*s != *p)
               goto starCheck;
            break;
      }
   }
   if (*p == '%') ++p;
   return (*p == 0);

starCheck:
   if (!star) return false;
   str++;
   goto loopStart;
}

bool Util::strCaseInsensitiveWildCmp(const char* str, const char* pat) {
   const char* s;
   const char* p;
   bool star = false;

loopStart:
   for (s = str, p = pat; *s != 0; ++s, ++p) {
      switch (*p) {
         case '?':
            break;
         case '%':
            star = true;
            str = s, pat = p;
            if (*++pat == 0) return true;
            goto loopStart;
         default:
            if (toLower(*s) != toLower(*p))
               goto starCheck;
            break;
      }
   }
   if (*p == '%') ++p;
   return (*p == 0);

starCheck:
   if (!star) return false;
   str++;
   goto loopStart;
}

bool Util::isWindows(){
#ifdef _WIN32
	return true;
#else
	return false;
#endif
}

int Util::getCoreCount(){
#ifdef _WIN32
	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	return sysinfo.dwNumberOfProcessors;
#else
	return sysconf(_SC_NPROCESSORS_ONLN);
#endif
}

long long Util::getPhysicalMemorySize() {
#ifdef _WIN32
	MEMORYSTATUSEX status;
	status.dwLength = sizeof(status);
	GlobalMemoryStatusEx(&status);
	return status.ullTotalPhys;
#else
	long pages = sysconf(_SC_PHYS_PAGES);
	long page_size = sysconf(_SC_PAGE_SIZE);
	return pages * page_size;
#endif
}

void Util::writeDoubleQuotedString(string& dest, const std::string & source){
	dest.append(1, '"');
	int len = static_cast<int>(source.length());
	for(int i=0; i<len; ++i){
		char ch = source[i];
		dest.append(ch=='"' ? 2 : 1, ch);
	}
	dest.append(1, '"');
}

void Util::sleep(int milliSeconds){
	if(milliSeconds <= 0)
		return;
#ifdef _WIN32
	::Sleep(milliSeconds);
#else
	usleep(1000 * milliSeconds);
#endif
}

int Util::getLastErrorCode(){
#ifdef _WIN32
	return WSAGetLastError();
#else
	return errno;
#endif
}
#ifdef _MSC_VER
std::string tcharToString(TCHAR *tchar){
#ifdef UNICODE
	int iLen = WideCharToMultiByte(CP_ACP, 0, tchar, -1, nullptr, 0, nullptr, nullptr);
	vector<char> chRtn(iLen * sizeof(char));
	WideCharToMultiByte(CP_ACP, 0, tchar, -1, chRtn.data(), iLen, nullptr, nullptr);
	return std::string(chRtn.begin(),chRtn.end());
#else
	return std::string(tchar);
#endif
}
#endif

std::string Util::getLastErrorMessage(){
#ifdef _WIN32
#ifdef _MSC_VER
	TCHAR buf[256];
	FormatMessage(
		FORMAT_MESSAGE_FROM_SYSTEM |
		FORMAT_MESSAGE_IGNORE_INSERTS,
		nullptr,
		WSAGetLastError(),
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		buf,
		256, nullptr);
	return tcharToString(buf);
#else
	char buf[256];
	FormatMessage(
		FORMAT_MESSAGE_FROM_SYSTEM |
		FORMAT_MESSAGE_IGNORE_INSERTS,
		nullptr,
		WSAGetLastError(),
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		buf,
		256, nullptr);
	return std::string(buf);
#endif
#else
	char buf[256]={0};
	char *msg=strerror_r(errno, buf, 256); // NOLINT(misc-include-cleaner): the suggested header is not Linux API
	return std::string(msg);
#endif
}

std::string Util::getErrorMessage(int errCode){
#ifdef _WIN32
#ifdef _MSC_VER
	TCHAR buf[256];
	FormatMessage(
		FORMAT_MESSAGE_FROM_SYSTEM |
		FORMAT_MESSAGE_IGNORE_INSERTS,
		nullptr,
		errCode,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		buf,
		256, nullptr );
	return tcharToString(buf);
#else
	char buf[256];
	FormatMessage(
		FORMAT_MESSAGE_FROM_SYSTEM |
		FORMAT_MESSAGE_IGNORE_INSERTS,
		nullptr,
		errCode,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		buf,
		256, nullptr);
	return std::string(buf);
#endif

#else
	std::ignore = errCode;
	char buf[256]={0};
	char *msg=strerror_r(errno, buf, 256);
	return std::string(msg);
#endif
}

std::string Util::getPartitionTypeString(PARTITION_TYPE type){
	return s_constFactory->getPartitionTypeString(type);
}

std::string Util::getCategoryString(DATA_CATEGORY type){
	return s_constFactory->getCategoryString(type);
}
Domain* Util::createDomain(PARTITION_TYPE type, DATA_TYPE partitionColType, const ConstantSP& partitionSchema){
	if(type == HASH){
		return new HashDomain(partitionColType, partitionSchema);
	}
	if(type == VALUE){
		return new ValueDomain(partitionColType, partitionSchema);
	}
	if(type == RANGE){
		return new RangeDomain(partitionColType, partitionSchema);
	}
	if(type == LIST){
		return new ListDomain(partitionColType, partitionSchema);
	}
	throw RuntimeException("Unsupported partition type " + getPartitionTypeString(type));
}
Vector* Util::createSubVector(const VectorSP& source, vector<int> indices){
	auto size = (INDEX)(indices.size());
	INDEX sourceSize = source->size();
	for(INDEX i = 0; i < size; i++){
		int index = indices[i];
		if(index < 0 || index >= sourceSize){
			throw RuntimeException("Failed to createSubVectot with index " + std::to_string(index));
		}
	}
	Vector* result = createVector(source->getType(), size, size, source->isFastMode(), source->getExtraParamForType());
	for(INDEX i = 0; i < size; i++){
		int index = indices[i];
		result->set(i, source->get(index));
	}
	return result;
}

Vector *Util::createSymbolVector(const SymbolBaseSP &symbolBase, INDEX size, INDEX capacity, bool fast, void *data,
                                 void **dataSegment, int segmentSizeInBit, bool containNull)
{
    if (size > 0) {
        symbolBase->find("");
    }
    std::ignore = fast;
    std::ignore = segmentSizeInBit;
    if (data == nullptr && dataSegment == nullptr) {
        try {
            data = (void *)new int[std::max(size, capacity)];
        } catch (...) {
            data = nullptr;
        }
    }
    if (data != nullptr)
        return new FastSymbolVector(symbolBase, size, capacity, (int *)data, containNull);
    return nullptr;
}

bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, std::nullptr_t val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	std::ignore = dataType;
	std::ignore = val;
	std::ignore = errorCodeInfo;
	std::ignore = extraParam;
	data->setNull();
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, Constant* val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	std::ignore = dataType;
	std::ignore = errorCodeInfo;
	std::ignore = extraParam;
	data = val;
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const ConstantSP& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	std::ignore = dataType;
	std::ignore = errorCodeInfo;
	std::ignore = extraParam;
	data = val;
	return true;
}

void Util::SetOrThrowErrorInfo(ErrorCodeInfo *errorCodeInfo, int errorCode, const string &errorInfo){
	if(errorCodeInfo != nullptr)
		errorCodeInfo->set(errorCode, errorInfo);
	else{
		throw RuntimeException(errorInfo);
	}
}

bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, bool val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	std::ignore = extraParam;
	switch (dataType) {
	case DATA_TYPE::DT_BOOL:
		data->setBool(static_cast<char>(val));
		return true;
	default:
		SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert bool to " + getDataTypeString(dataType));
		break;
	}
	return false;
}

bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, long long val, const char *pTypeName, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	std::ignore = extraParam;
	switch (dataType) {
	case DATA_TYPE::DT_DECIMAL128:
		static_cast<SmartPointer<Decimal128>>(data)->setLong(val);
		break;
	case DATA_TYPE::DT_DECIMAL64:
		static_cast<SmartPointer<Decimal64>>(data)->setLong(val);
		break;
	case DATA_TYPE::DT_LONG:
		data->setLong(val);
		break;
	case DATA_TYPE::DT_DECIMAL32:
		static_cast<SmartPointer<Decimal32>>(data)->setLong(val);
		break;
	case DATA_TYPE::DT_INT:
		if(val >= INT_MIN && val <= INT_MAX){
			data->setInt(static_cast<int>(val));
		} else {
			SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, std::string(pTypeName) + " cannot be converted because it exceeds the range of " + getDataTypeString(dataType));
			return false;
		}
		break;
	case DATA_TYPE::DT_SHORT:
		if(val >= SHRT_MIN && val <= SHRT_MAX)
			data->setShort(static_cast<short>(val));
		else {
			SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, std::string(pTypeName) + " cannot be converted because it exceeds the range of  " + getDataTypeString(dataType));
			return false;
		}
		break;
	case DATA_TYPE::DT_CHAR:
		if(val >= SCHAR_MIN && val <= SCHAR_MAX)
			data->setChar(static_cast<char>(val));
		else {
			SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, std::string(pTypeName) + " cannot be converted because it exceeds the range of  " + getDataTypeString(dataType));
			return false;
		}
		break;
	default:
		SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert "+std::string(pTypeName) +" to " + getDataTypeString(dataType));
		return false;
	}
	return true;
}

bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, char val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	if (LIKE_TYPE(dataType, DT_CHAR) == DATA_TYPE::DT_BOOL) {
		data->setBool(val);
		return true;
	}
	return setValue(data, dataType,(long long)val,"char", errorCodeInfo, extraParam);
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, short val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	return setValue(data, dataType, (long long)val, "short", errorCodeInfo, extraParam);
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const char* val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	std::ignore = extraParam;
	if (val != (const void*)nullptr) {
		switch (LIKE_TYPE(dataType, DT_STRING)) {
		case DATA_TYPE::DT_INT128:
		{
			unsigned char buf[16];
			bool flag = Int128::parseInt128(val, strlen(val), buf);
			if (!flag) {
				SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert string to " + getDataTypeString(dataType));
				return false;
			}
			static_cast<SmartPointer<Int128>>(data)->setBinary(buf, 16);
		}
		break;
		case DATA_TYPE::DT_UUID:
		{
			unsigned char buf[16];
			bool flag = Uuid::parseUuid(val, strlen(val), buf);
			if (!flag) {
				SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert string to " + getDataTypeString(dataType));
				return false;
			}
			static_cast<SmartPointer<Uuid>>(data)->setBinary(buf, 16);
		}
		break;
		case DATA_TYPE::DT_IP:
		{
			unsigned char buf[16];
			bool flag = IPAddr::parseIPAddr(val, strlen(val), buf);
			if (!flag) {
				SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert string to " + getDataTypeString(dataType));
				return false;
			}
			static_cast<SmartPointer<IPAddr>>(data)->setBinary(buf, 16);
		}
		break;
		case DATA_TYPE::DT_SYMBOL:
		case DATA_TYPE::DT_STRING:
		case DATA_TYPE::DT_BLOB:
		{
			data->setString(val);
		}
		break;
		default:
			SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert pointer to " + getDataTypeString(dataType));
			return false;
			break;
		}
	}
	else {
		data->setNull();
	}
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const void* val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	std::ignore = extraParam;
	if (val != (const void*)nullptr) {
		switch (dataType) {
		case DATA_TYPE::DT_DECIMAL32:
		{
			static_cast<SmartPointer<Decimal32>>(data)->setRawData(*(int32_t*)val);
		}
		break;
		case DATA_TYPE::DT_DECIMAL64:
		{
			static_cast<SmartPointer<Decimal64>>(data)->setRawData(*(int64_t*)val);
		}
		break;
		case DATA_TYPE::DT_DECIMAL128:
		{
			static_cast<SmartPointer<Decimal128>>(data)->setRawData(*(wide_integer::int128*)val);
		}
		break;
		case DATA_TYPE::DT_INT128:
		{
			static_cast<SmartPointer<Int128>>(data)->setBinary((const unsigned char*)val, 16);
		}
		break;
		case DATA_TYPE::DT_UUID:
		{
			static_cast<SmartPointer<Uuid>>(data)->setBinary((const unsigned char*)val, 16);
		}
		break;
		case DATA_TYPE::DT_IP:
		{
			static_cast<SmartPointer<IPAddr>>(data)->setBinary((const unsigned char*)val, 16);
		}
		break;
		case DATA_TYPE::DT_SYMBOL:
		case DATA_TYPE::DT_STRING:
		case DATA_TYPE::DT_BLOB:
		{
			data->setString((const char*)val);
		}
		break;
		default:
			SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert pointer to " + getDataTypeString(dataType));
			return false;
		}
	}
	else {
		data->setNull();
	}
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, std::string val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	return setValue(data, dataType, (const char *)val.data(), errorCodeInfo, extraParam);
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const unsigned char* val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	return setValue(data, dataType, (const void *)val, errorCodeInfo, extraParam);
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, unsigned char val[], ErrorCodeInfo &errorCodeInfo, int extraParam) {
	return setValue(data, dataType, (const void *)val, errorCodeInfo, extraParam);
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, char val[], ErrorCodeInfo &errorCodeInfo, int extraParam) {
    return setValue(data, dataType, (const void *)val, errorCodeInfo, extraParam);
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, long int val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	return setValue(data, dataType, (long long)val, errorCodeInfo, extraParam);
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, long long val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	switch (LIKE_TYPE(dataType, DT_NANOTIMESTAMP)) {
	case DATA_TYPE::DT_NANOTIME:
	case DATA_TYPE::DT_NANOTIMESTAMP:
	case DATA_TYPE::DT_TIMESTAMP:
		data->setLong(val);
		break;
	case DATA_TYPE::DT_DATE:
	case DATA_TYPE::DT_MONTH:
	case DATA_TYPE::DT_TIME:
	case DATA_TYPE::DT_SECOND:
	case DATA_TYPE::DT_MINUTE:
	case DATA_TYPE::DT_DATETIME:
	case DATA_TYPE::DT_DATEHOUR:
		data->setInt(static_cast<int>(val));
		break;
	default:
		return setValue(data, dataType, val, "long", errorCodeInfo, extraParam);
	}
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, float val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	std::ignore = extraParam;
	switch (LIKE_TYPE(dataType, DT_FLOAT)) {
	case DATA_TYPE::DT_DECIMAL32:
	case DATA_TYPE::DT_DECIMAL64:
	case DATA_TYPE::DT_DECIMAL128:
	case DATA_TYPE::DT_FLOAT:
	case DATA_TYPE::DT_DOUBLE:
		data->setFloat(val);
		break;
	default:
		SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert float to " + getDataTypeString(dataType));
		return false;
	}
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, double val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	std::ignore = extraParam;
	switch (LIKE_TYPE(dataType, DT_DOUBLE)) {
	case DATA_TYPE::DT_FLOAT:
		if(val >= FLT_MIN && val <= FLT_MAX)
			data->setFloat(static_cast<float>(val));
		else {
			SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert double to " + getDataTypeString(dataType));
			return false;
		}
		break;
	case DATA_TYPE::DT_DECIMAL32:
	case DATA_TYPE::DT_DECIMAL64:
	case DATA_TYPE::DT_DECIMAL128:
	case DATA_TYPE::DT_DOUBLE:
		data->setDouble(val);
		break;
	default:
		SetOrThrowErrorInfo(&errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert double to " + getDataTypeString(dataType));
		return false;
	}
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, int val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	switch (LIKE_TYPE(dataType, DT_INT)) {
	case DATA_TYPE::DT_DATE:
	case DATA_TYPE::DT_MONTH:
	case DATA_TYPE::DT_TIME:
	case DATA_TYPE::DT_SECOND:
	case DATA_TYPE::DT_MINUTE:
	case DATA_TYPE::DT_DATETIME:
	case DATA_TYPE::DT_DATEHOUR:
	case DATA_TYPE::DT_INT:
		data->setInt(val);
		break;
	default:
		return setValue(data, dataType,(long long)val,"int", errorCodeInfo, extraParam);
	}
	return true;
}
template<class T>
ConstantSP createVectorObject(DATA_TYPE dataType, const std::vector<T>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	VectorSP dataVector = Util::createVector(dataType, 0, static_cast<INDEX>(val.size()), true, extraParam);
	for (const auto &one : val) {
		ConstantSP csp = Util::createObject(dataType, one, errorCodeInfo, extraParam);
		if (!csp.isNull())
			dataVector->append(csp);
		else
			return nullptr;
	}
	VectorSP anyVector = Util::createVector(DT_ANY, 0, 1);
	anyVector->append((ConstantSP)dataVector);
	return (ConstantSP)anyVector;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<std::nullptr_t>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<std::nullptr_t>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<Constant*>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<Constant*>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<ConstantSP>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<ConstantSP>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<bool>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<bool>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<char>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<char>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<short>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<short>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<const char*>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<const char*>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<std::string>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<std::string>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<const unsigned char*>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<const unsigned char*>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<long long>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<long long>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<long int>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<long int>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<int>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<int>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<float>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<float>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<double>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<double>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}
bool Util::setValue(ConstantSP& data, DATA_TYPE dataType, const std::vector<const void*>& val, ErrorCodeInfo &errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	data = createVectorObject<const void*>(dataType, val, &errorCodeInfo, extraParam);
	return true;
}

ConstantSP Util::createObject(DATA_TYPE dataType, std::nullptr_t val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	std::ignore = val;
	std::ignore = extraParam;
	std::ignore = errorCodeInfo;
	return createNullConstant(dataType);
}
ConstantSP Util::createObject(DATA_TYPE dataType, Constant* val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	std::ignore = dataType;
	std::ignore = errorCodeInfo;
	std::ignore = extraParam;
	return val;
}
ConstantSP Util::createObject(DATA_TYPE dataType, const ConstantSP& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	std::ignore = dataType;
	std::ignore = errorCodeInfo;
	std::ignore = extraParam;
	return val;
}


ConstantSP Util::createObject(DATA_TYPE dataType, bool val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	std::ignore = extraParam;
	switch (dataType) {
	case DATA_TYPE::DT_BOOL:
		return createBool(val);
		break;
	default:
		SetOrThrowErrorInfo(errorCodeInfo,ErrorCodeInfo::EC_InvalidObject, "Cannot convert bool to " + getDataTypeString(dataType));
		return nullptr;
		break;
	}
}

ConstantSP Util::createValue(DATA_TYPE dataType, long long val, const char *pTypeName, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	switch (dataType) {
	case DATA_TYPE::DT_DECIMAL128:
		if (extraParam > 0)
			return createDecimal128(extraParam, (double)val);
		return createDouble(static_cast<double>(val));
		break;
	case DATA_TYPE::DT_DECIMAL64:
		if (extraParam > 0)
			return createDecimal64(extraParam, (double)val);
		return createDouble(static_cast<double>(val));
		break;
	case DATA_TYPE::DT_LONG:
		return createLong(val);
		break;
	case DATA_TYPE::DT_DECIMAL32:
		if (extraParam > 0)
			return createDecimal32(extraParam, (double)val);
		return createDouble(static_cast<double>(val));
		break;
	case DATA_TYPE::DT_INT:
		if(val >= INT_MIN && val <= INT_MAX)
			return createInt(static_cast<int>(val));
		else {
			SetOrThrowErrorInfo(errorCodeInfo,ErrorCodeInfo::EC_InvalidObject, std::string(pTypeName) + " cannot be converted because it exceeds the range of " + getDataTypeString(dataType));
		}
		break;
	case DATA_TYPE::DT_SHORT:
		if(val >= SHRT_MIN && val <= SHRT_MAX)
			return createShort(static_cast<short>(val));
		else {
			SetOrThrowErrorInfo(errorCodeInfo,ErrorCodeInfo::EC_InvalidObject, std::string(pTypeName) + " cannot be converted because it exceeds the range of  " + getDataTypeString(dataType));
		}
		break;
	case DATA_TYPE::DT_CHAR:
		if(val >= SCHAR_MIN && val <= SCHAR_MAX)
			return createChar((char)val);
		else {
			SetOrThrowErrorInfo(errorCodeInfo,ErrorCodeInfo::EC_InvalidObject, std::string(pTypeName) + " cannot be converted because it exceeds the range of  " + getDataTypeString(dataType));
		}
		break;
	default:
		SetOrThrowErrorInfo(errorCodeInfo,ErrorCodeInfo::EC_InvalidObject, "Cannot convert "+std::string(pTypeName) +" to " + getDataTypeString(dataType));
		break;
	}
	return nullptr;
}

ConstantSP Util::createObject(DATA_TYPE dataType, char val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	switch (dataType) {
	case DATA_TYPE::DT_BOOL:
		return createBool(val);
		break;
	default:
		return createValue(dataType,(long long)val,"char", errorCodeInfo, extraParam);
		break;
	}
}
ConstantSP Util::createObject(DATA_TYPE dataType, short val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	return createValue(dataType,(long long)val,"short", errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const char* val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	std::ignore = extraParam;
	if (val != (const void*)nullptr) {
		switch (dataType) {
		case DATA_TYPE::DT_INT128:
		{
			Int128 *pint128 = Int128::parseInt128(val, strlen(val));
			if (pint128 == nullptr) {
				SetOrThrowErrorInfo(errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert string to " + getDataTypeString(dataType));
			}
			return pint128;
		}
		break;
		case DATA_TYPE::DT_UUID:
		{
			Uuid *puuid = Uuid::parseUuid(val, strlen(val));
			if (puuid == nullptr) {
				SetOrThrowErrorInfo(errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert string to " + getDataTypeString(dataType));
			}
			return puuid;
		}
		break;
		case DATA_TYPE::DT_IP:
		{
			IPAddr *pip = IPAddr::parseIPAddr(val, strlen(val));
			if (pip == nullptr) {
				SetOrThrowErrorInfo(errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert string to " + getDataTypeString(dataType));
			}
			return pip;
		}
		break;
		case DATA_TYPE::DT_SYMBOL:
		{
			ConstantSP tmp = createConstant(DATA_TYPE::DT_SYMBOL);
			tmp->setString(val);
			return tmp;
		}
		break;
		case DATA_TYPE::DT_STRING:
			return createString(val);
		case DATA_TYPE::DT_BLOB:
		{
			ConstantSP tmp = createConstant(DATA_TYPE::DT_BLOB);
			tmp->setString(val);
			return tmp;
		}
		default:
			SetOrThrowErrorInfo(errorCodeInfo, ErrorCodeInfo::EC_InvalidObject, "Cannot convert pointer to " + getDataTypeString(dataType));
			break;
		}
	}
	else {
		return createNullConstant(dataType);
	}
	return nullptr;
}
ConstantSP Util::createObject(DATA_TYPE dataType, const void* val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	if (val != (const void*)nullptr) {
		switch (dataType) {
		case DATA_TYPE::DT_DECIMAL32:
		{
			auto *pdecimal = new Decimal32(extraParam, *(int32_t*)val);
			return pdecimal;
		}
		break;
		case DATA_TYPE::DT_DECIMAL64:
		{
			auto *pdecimal = new Decimal64(extraParam, *(int64_t*)val);
			return pdecimal;
		}
		break;
		case DATA_TYPE::DT_DECIMAL128:
		{
			auto *pdecimal = new Decimal128(extraParam, *(wide_integer::int128*)val);
			return pdecimal;
		}
		break;
		case DATA_TYPE::DT_INT128:
		{
			ConstantSP tmp = createConstant(DATA_TYPE::DT_INT128);
			tmp->setBinary((const unsigned char*)val, 16);
			return tmp;
		}
		break;
		case DATA_TYPE::DT_UUID:
		{
			ConstantSP tmp = createConstant(DATA_TYPE::DT_UUID);
			tmp->setBinary((const unsigned char*)val, 16);
			return tmp;
		}
		break;
		case DATA_TYPE::DT_IP:
		{
			ConstantSP tmp = createConstant(DATA_TYPE::DT_IP);
			tmp->setBinary((const unsigned char*)val, 16);
			return tmp;
		}
		break;
		case DATA_TYPE::DT_SYMBOL:
		{
			ConstantSP tmp = createConstant(DATA_TYPE::DT_SYMBOL);
			tmp->setString((const char*)val);
			return tmp;
		}
		break;
		case DATA_TYPE::DT_STRING:
			return createString((const char*)val);
		case DATA_TYPE::DT_BLOB:
		{
			ConstantSP tmp = createConstant(DATA_TYPE::DT_BLOB);
			tmp->setString((const char*)val);
			return tmp;
		}
		default:
			SetOrThrowErrorInfo(errorCodeInfo,ErrorCodeInfo::EC_InvalidObject, "Cannot convert pointer to " + getDataTypeString(dataType));
			break;
		}
	}
	else {
		return createNullConstant(dataType);
	}
	return nullptr;
}
ConstantSP Util::createObject(DATA_TYPE dataType, std::string val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	return createObject(dataType, (const char *)val.data(), errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const unsigned char* val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	return createObject(dataType, (const void *)val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, unsigned char val[], ErrorCodeInfo *errorCodeInfo, int extraParam) {
	return createObject(dataType, (const void *)val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, char val[], ErrorCodeInfo *errorCodeInfo, int extraParam) {
    return createObject(dataType, (const void *)val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, long int val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	return createObject(dataType, (long long)val, errorCodeInfo, extraParam);
}
//ConstantSP Util::createObject(DATA_TYPE dataType, long val){
//    return createObject(dataType, (long long)val);
//}
ConstantSP Util::createObject(DATA_TYPE dataType, long long val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	switch (dataType) {
	case DATA_TYPE::DT_NANOTIME:
		return createNanoTime(val);
		break;
	case DATA_TYPE::DT_NANOTIMESTAMP:
		return createNanoTimestamp(val);
		break;
	case DATA_TYPE::DT_TIMESTAMP:
		return createTimestamp(val);
		break;
	case DATA_TYPE::DT_DATE:
		return createDate(static_cast<int>(val));
		break;
	case DATA_TYPE::DT_MONTH:
		return createMonth(static_cast<int>(val));
		break;
	case DATA_TYPE::DT_TIME:
		return createTime(static_cast<int>(val));
		break;
	case DATA_TYPE::DT_SECOND:
		return createSecond(static_cast<int>(val));
		break;
	case DATA_TYPE::DT_MINUTE:
		return createMinute(static_cast<int>(val));
		break;
	case DATA_TYPE::DT_DATETIME:
		return createDateTime(static_cast<int>(val));
		break;
	case DATA_TYPE::DT_DATEHOUR:
		return createDateHour(static_cast<int>(val));
		break;
	default:
		return createValue(dataType,val,"long", errorCodeInfo, extraParam);
		break;
	}
}
ConstantSP Util::createObject(DATA_TYPE dataType, float val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	switch (dataType) {
	case DATA_TYPE::DT_DECIMAL32:
		if (extraParam > 0)
			return createDecimal32(extraParam, val);
		return createDouble(val);
		break;
	case DATA_TYPE::DT_DECIMAL64:
		if(extraParam > 0)
			return createDecimal64(extraParam, val);
		return createDouble(val);
		break;
	case DATA_TYPE::DT_DECIMAL128:
		if(extraParam > 0)
			return createDecimal128(extraParam, val);
		return createDouble(val);
		break;
	case DATA_TYPE::DT_FLOAT:
		return createFloat(val);
		break;
	case DATA_TYPE::DT_DOUBLE:
		return createDouble(val);
		break;
	default:
		SetOrThrowErrorInfo(errorCodeInfo,ErrorCodeInfo::EC_InvalidObject, "Cannot convert float to " + getDataTypeString(dataType));
		break;
	}
	return nullptr;
}
ConstantSP Util::createObject(DATA_TYPE dataType, double val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	switch (dataType) {
	case DATA_TYPE::DT_FLOAT:
		if(val >= FLT_MIN && val <= FLT_MAX)
			return createFloat(static_cast<float>(val));
		else {
			SetOrThrowErrorInfo(errorCodeInfo,ErrorCodeInfo::EC_InvalidObject, "Cannot convert double to " + getDataTypeString(dataType));
		}
		break;
	case DATA_TYPE::DT_DECIMAL32:
		if(extraParam > 0)
			return createDecimal32(extraParam, val);
		return createDouble(val);
		break;
	case DATA_TYPE::DT_DECIMAL64:
		if (extraParam > 0)
			return createDecimal64(extraParam, val);
		return createDouble(val);

		break;
	case DATA_TYPE::DT_DECIMAL128:
		if (extraParam > 0)
			return createDecimal128(extraParam, val);
		return createDouble(val);

		break;
	case DATA_TYPE::DT_DOUBLE:
		return createDouble(val);
		break;
	default:
		SetOrThrowErrorInfo(errorCodeInfo,ErrorCodeInfo::EC_InvalidObject, "Cannot convert double to " + getDataTypeString(dataType));
		break;
	}
	return nullptr;
}
ConstantSP Util::createObject(DATA_TYPE dataType, int val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	switch (dataType) {
	case DATA_TYPE::DT_DATE:
		return createDate(val);
		break;
	case DATA_TYPE::DT_MONTH:
		return createMonth(val);
		break;
	case DATA_TYPE::DT_TIME:
		return createTime(val);
		break;
	case DATA_TYPE::DT_SECOND:
		return createSecond(val);
		break;
	case DATA_TYPE::DT_MINUTE:
		return createMinute(val);
		break;
	case DATA_TYPE::DT_DATETIME:
		return createDateTime(val);
		break;
	case DATA_TYPE::DT_DATEHOUR:
		return createDateHour(val);
		break;
	default:
		return createValue(dataType,(long long)val,"int", errorCodeInfo, extraParam);
		break;
	}
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<std::nullptr_t>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<std::nullptr_t>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<Constant*>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<Constant*>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<ConstantSP>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<ConstantSP>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<bool>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<bool>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<char>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<char>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<short>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<short>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<const char*>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<const char*>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<std::string>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<std::string>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<const unsigned char*>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<const unsigned char*>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<long long>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<long long>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<long int>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<long int>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<int>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<int>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<float>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<float>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<double>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<double>(dataType, val, errorCodeInfo, extraParam);
}
ConstantSP Util::createObject(DATA_TYPE dataType, const std::vector<const void*>& val, ErrorCodeInfo *errorCodeInfo, int extraParam) {
	//Only arrayVector needs vector data and it requires any vector.
	return createVectorObject<const void*>(dataType, val, errorCodeInfo, extraParam);
}

bool Util::checkColDataType(DATA_TYPE colDataType, bool isColTemporal,ConstantSP &constsp) {
	if (colDataType >= ARRAY_TYPE_BASE) {//arrayVector
		return constsp->getForm() == DF_VECTOR;//needs DT_ANY vector
	}
	if (constsp->getForm() == DF_SCALAR) {
		if (constsp->getType() == colDataType)
			return true;
		if (colDataType == DT_SYMBOL && constsp->getType() == DT_STRING)
			return true;
		if (isColTemporal && constsp->isTemporary()) // server can convert between different Temporal
			return true;
	}
	return false;
}

unsigned long Util::getCurThreadId() {
#ifdef _WIN32
	return GetCurrentThreadId();
#elif defined MAC
	return syscall(SYS_thread_selfid);
#else
	return pthread_self();
#endif
}

void Util::writeFile(const char *pfilepath, const void *pbytes, std::size_t bytelen){
	if(bytelen < 1)
		return;
	FILE *pf = nullptr;
#ifdef _MSC_VER
	fopen_s(&pf, pfilepath, "ab");
#else
	pf = fopen(pfilepath,"ab");
#endif
	if(pf == nullptr)
		return;
	fwrite(pbytes, bytelen, 1, pf);
	fclose(pf);
}

} // namespace dolphindb
