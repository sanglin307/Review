// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "SmartPointer.h"
#include "Exceptions.h"
#include "Guid.h"
#include "SymbolBase.h"
#include "Types.h"
#include "internal/WideInteger.h"
#include <string>

#if defined(_MSC_VER)
#pragma warning( push )
#pragma warning( disable : 4100 4251 )
#elif defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#else // gcc
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

#define NOT_IMPLEMENT \
    throw RuntimeException("Data type [" + std::to_string(static_cast<int>(getType())) + "] form [" + \
            std::to_string(static_cast<int>(getForm())) + "] does not implement `" + __func__ + "`");

#if defined(__GNUC__) && __GNUC__ >= 4
    #define LIKELY(x) (__builtin_expect((x), 1))
    #define UNLIKELY(x) (__builtin_expect((x), 0))
#else
    #define LIKELY(x) (x)
    #define UNLIKELY(x) (x)
#endif

namespace dolphindb {

constexpr uint16_t FLAG_TEMPORARY   {1 << 0U};
constexpr uint16_t FLAG_INDEPENDENT {1 << 1U};
constexpr uint16_t FLAG_READONLY    {1 << 2U};
// constexpr uint16_t UNUSED_FLAG   {1 << 3U};
constexpr uint16_t FLAG_NOTHING     {1 << 4U};

class Constant;
using ConstantSP = SmartPointer<Constant>;

class EXPORT_DECL Constant {
public:
    static std::string EMPTY;
    static std::string NULL_STR;
    static ConstantSP void_;
    static ConstantSP null_;
    static ConstantSP true_;
    static ConstantSP false_;
    static ConstantSP one_;

    Constant() : flag_(3){}
    explicit Constant(unsigned short flag) :  flag_(flag){}
    virtual ~Constant() = default;
    virtual bool isLargeConstant() const {return false;}
    bool isTemporary() const {return flag_ & FLAG_TEMPORARY;}
    void setTemporary(bool val){ if(val) flag_ |= FLAG_TEMPORARY; else flag_ &= (uint16_t)~FLAG_TEMPORARY;}
    bool isIndependent() const {return flag_ & FLAG_INDEPENDENT;}
    void setIndependent(bool val){ if(val) flag_ |= FLAG_INDEPENDENT; else flag_ &= (uint16_t)~FLAG_INDEPENDENT;}
    bool isReadOnly() const {return flag_ & FLAG_READONLY;}
    void setReadOnly(bool val){ if(val) flag_ |= FLAG_READONLY; else flag_ &= (uint16_t)~FLAG_READONLY;}
    bool isNothing() const {return flag_ & FLAG_NOTHING;}
    void setNothing(bool val){ if(val) flag_ |= FLAG_NOTHING; else flag_ &= (uint16_t)~FLAG_NOTHING;}
    DATA_FORM getForm() const {return DATA_FORM(flag_ >> 8);}
    void setForm(DATA_FORM df){ flag_ = (flag_ & static_cast<unsigned short>(127)) + static_cast<unsigned short>(df << 8);}
    bool isScalar() const { return getForm()==DF_SCALAR;}
    bool isArray() const { return getForm()==DF_VECTOR;}
    bool isPair() const { return getForm()==DF_PAIR;}
    bool isMatrix() const {return getForm()==DF_MATRIX;}
    //a vector could be array, pair or matrix.
    bool isVector() const { return getForm()>=DF_VECTOR && getForm()<=DF_MATRIX;}
    bool isTable() const { return getForm()==DF_TABLE;}
    bool isSet() const {return getForm()==DF_SET;}
    bool isDictionary() const {return getForm()==DF_DICTIONARY;}
    bool isChunk() const {return getForm()==DF_CHUNK;}
    bool isTuple() const {return getForm()==DF_VECTOR && getType()==DT_ANY;}
    bool isNumber() const { DATA_CATEGORY cat = getCategory(); return cat == INTEGRAL || cat == FLOATING;}

    virtual bool isDatabase() const {return false;}
    virtual char getBool() const {throw RuntimeException("The object can't be converted to boolean scalar.");}
    virtual char getChar() const {throw RuntimeException("The object can't be converted to char scalar.");}
    virtual short getShort() const {throw RuntimeException("The object can't be converted to short scalar.");}
    virtual int getInt() const {throw RuntimeException("The object can't be converted to int scalar.");}
    virtual long long  getLong() const {throw RuntimeException("The object can't be converted to long scalar.");}
    virtual INDEX getIndex() const {throw RuntimeException("The object can't be converted to index scalar.");}
    virtual float getFloat() const {throw RuntimeException("The object can't be converted to float scalar.");}
    virtual double getDouble() const {throw RuntimeException("The object can't be converted to double scalar.");}
    virtual std::string getString() const {return "";}
    virtual std::string getScript() const { return getString();}
    virtual const std::string& getStringRef() const {return EMPTY;}
    virtual Guid getUuid() const {return getInt128();}
    virtual Guid getInt128() const {throw RuntimeException("The object can't be converted to uuid scalar.");}
    virtual const unsigned char* getBinary() const {throw RuntimeException("The object can't be converted to int128 scalar.");}
    virtual bool isNull() const {return false;}
    virtual int getHash(int buckets) const {return -1;}

    virtual void setBool(char val){}
    virtual void setChar(char val){}
    virtual void setShort(short val){}
    virtual void setInt(int val){}
    virtual void setLong(long long val){}
    virtual void setIndex(INDEX val){}
    virtual void setFloat(float val){}
    virtual void setDouble(double val){}
    virtual void setString(const std::string& val){}
    virtual void setBinary(const unsigned char* val, int unitLength){}
    virtual void setNull(){}

    virtual char getBool(INDEX index) const {return getBool();}
    virtual char getChar(INDEX index) const { return getChar();}
    virtual short getShort(INDEX index) const { return getShort();}
    virtual int getInt(INDEX index) const {return getInt();}
    virtual long long getLong(INDEX index) const {return getLong();}
    virtual INDEX getIndex(INDEX index) const {return getIndex();}
    virtual float getFloat(INDEX index) const {return getFloat();}
    virtual double getDouble(INDEX index) const {return getDouble();}
    virtual std::string getString(INDEX index) const {return getString();}
    virtual const std::string& getStringRef(INDEX index) const {return EMPTY;}
    virtual bool isNull(INDEX index) const {return isNull();}

    virtual int32_t getDecimal32(INDEX index, int scale) const { NOT_IMPLEMENT; }
    virtual int64_t getDecimal64(INDEX index, int scale) const { NOT_IMPLEMENT; }
    virtual wide_integer::int128 getDecimal128(INDEX index, int scale) const { NOT_IMPLEMENT; }

    virtual ConstantSP get(INDEX index) const {return getValue();}
    virtual ConstantSP get(INDEX column, INDEX row) const {return get(row);}
    virtual ConstantSP get(const ConstantSP& index) const {return getValue();}
    virtual ConstantSP getColumn(INDEX index) const {return getValue();}
    virtual ConstantSP getRow(INDEX index) const {return get(index);}
    virtual ConstantSP getItem(INDEX index) const {return get(index);}
    virtual ConstantSP getWindow(INDEX colStart, int colLength, INDEX rowStart, int rowLength) const {return getValue();}
    virtual ConstantSP getRowLabel() const;
    virtual ConstantSP getColumnLabel() const;

    virtual bool isNull(INDEX start, int len, char* buf) const {return false;}
    virtual bool isValid(INDEX start, int len, char* buf) const {return false;}
    virtual bool getBool(INDEX start, int len, char* buf) const {return false;}
    virtual bool getChar(INDEX start, int len,char* buf) const {return false;}
    virtual bool getShort(INDEX start, int len, short* buf) const {return false;}
    virtual bool getInt(INDEX start, int len, int* buf) const {return false;}
    virtual bool getLong(INDEX start, int len, long long* buf) const {return false;}
    virtual bool getIndex(INDEX start, int len, INDEX* buf) const {return false;}
    virtual bool getFloat(INDEX start, int len, float* buf) const {return false;}
    virtual bool getDouble(INDEX start, int len, double* buf) const {return false;}
    virtual bool getSymbol(INDEX start, int len, int* buf, SymbolBase* symBase,bool insertIfNotThere) const {return false;}
    virtual bool getString(INDEX start, int len, std::string** buf) const {return false;}
    virtual bool getString(INDEX start, int len, char** buf) const {return false;}
    virtual bool getBinary(INDEX start, int len, int unitLength, unsigned char* buf) const {return false;}
    virtual bool getHash(INDEX start, int len, int buckets, int* buf) const {return false;}
    virtual bool getDecimal32(INDEX start, int len, int scale, int32_t *buf) const { NOT_IMPLEMENT; }
    virtual bool getDecimal64(INDEX start, int len, int scale, int64_t *buf) const { NOT_IMPLEMENT; }
    virtual bool getDecimal128(INDEX start, int len, int scale, wide_integer::int128 *buf) const { NOT_IMPLEMENT; }

    virtual const char* getBoolConst(INDEX start, int len, char* buf) const {throw RuntimeException("getBoolConst method not supported");}
    virtual const char* getCharConst(INDEX start, int len,char* buf) const {throw RuntimeException("getCharConst method not supported");}
    virtual const short* getShortConst(INDEX start, int len, short* buf) const {throw RuntimeException("getShortConst method not supported");}
    virtual const int* getIntConst(INDEX start, int len, int* buf) const {throw RuntimeException("getIntConst method not supported");}
    virtual const long long* getLongConst(INDEX start, int len, long long* buf) const {throw RuntimeException("getLongConst method not supported");}
    virtual const INDEX* getIndexConst(INDEX start, int len, INDEX* buf) const {throw RuntimeException("getIndexConst method not supported");}
    virtual const float* getFloatConst(INDEX start, int len, float* buf) const {throw RuntimeException("getFloatConst method not supported");}
    virtual const double* getDoubleConst(INDEX start, int len, double* buf) const {throw RuntimeException("getDoubleConst method not supported");}
    virtual const int* getSymbolConst(INDEX start, int len, int* buf, SymbolBase* symBase, bool insertIfNotThere) const {throw RuntimeException("getSymbolConst method not supported");}
    virtual std::string** getStringConst(INDEX start, int len, std::string** buf) const {throw RuntimeException("getStringConst method not supported");}
    virtual char** getStringConst(INDEX start, int len, char** buf) const {throw RuntimeException("getStringConst method not supported");}
    virtual const unsigned char* getBinaryConst(INDEX start, int len, int unitLength, unsigned char* buf) const {throw RuntimeException("getBinaryConst method not supported");}

    virtual char* getBoolBuffer(INDEX start, int len, char* buf) const {return buf;}
    virtual char* getCharBuffer(INDEX start, int len,char* buf) const {return buf;}
    virtual short* getShortBuffer(INDEX start, int len, short* buf) const {return buf;}
    virtual int* getIntBuffer(INDEX start, int len, int* buf) const {return nullptr;}
    virtual long long* getLongBuffer(INDEX start, int len, long long* buf) const {return buf;}
    virtual INDEX* getIndexBuffer(INDEX start, int len, INDEX* buf) const {return buf;}
    virtual float* getFloatBuffer(INDEX start, int len, float* buf) const {return buf;}
    virtual double* getDoubleBuffer(INDEX start, int len, double* buf) const {return buf;}
    virtual unsigned char* getBinaryBuffer(INDEX start, int len, int unitLength, unsigned char* buf) const {return buf;}
    virtual void* getDataBuffer(INDEX start, int len, void* buf) const {return buf;}

    virtual int serialize(char* buf, int bufSize, INDEX indexStart, int offset, int cellCountToSerialize, int& numElement, int& partial) const;
    virtual int serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const;
    virtual IO_ERR deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement);

    virtual void nullFill(const ConstantSP& val){}
    virtual void setBool(INDEX index,bool val){setBool(val);}
    virtual void setChar(INDEX index,char val){setChar(val);}
    virtual void setShort(INDEX index,short val){setShort(val);}
    virtual void setInt(INDEX index,int val){setInt(val);}
    virtual void setLong(INDEX index,long long val){setLong(val);}
    virtual void setIndex(INDEX index,INDEX val){setIndex(val);}
    virtual void setFloat(INDEX index,float val){setFloat(val);}
    virtual void setDouble(INDEX index, double val){setDouble(val);}
    virtual void setString(INDEX index, const std::string& val){setString(val);}
    virtual void setBinary(INDEX index, int unitLength, const unsigned char* val){setBinary(val, unitLength);}
    virtual void setNull(INDEX index){setNull();}

    virtual bool set(INDEX index, const ConstantSP& value){return false;}
    virtual bool set(INDEX column, INDEX row, const ConstantSP& value){return false;}
    virtual bool set(const ConstantSP& index, const ConstantSP& value) {return false;}
    virtual bool setItem(INDEX index, const ConstantSP& value){return set(index,value);}
    virtual bool setColumn(INDEX index, const ConstantSP& value){return assign(value);}
    virtual void setRowLabel(const ConstantSP& label){}
    virtual void setColumnLabel(const ConstantSP& label){}
    virtual bool reshape(INDEX colNum, INDEX rowNum) {return false;}
    virtual bool assign(const ConstantSP& value){return false;}

    virtual bool setBool(INDEX start, int len, const char* buf){return false;}
    virtual bool setChar(INDEX start, int len, const char* buf){return false;}
    virtual bool setShort(INDEX start, int len, const short* buf){return false;}
    virtual bool setInt(INDEX start, int len, const int* buf){return false;}
    virtual bool setLong(INDEX start, int len, const long long* buf){return false;}
    virtual bool setIndex(INDEX start, int len, const INDEX* buf){return false;}
    virtual bool setFloat(INDEX start, int len, const float* buf){return false;}
    virtual bool setDouble(INDEX start, int len, const double* buf){return false;}
    virtual bool setString(INDEX start, int len, const std::string* buf){return false;}
    virtual bool setString(INDEX start, int len, char** buf){return false;}
    virtual bool setBinary(INDEX start, int len, int unitLength, const unsigned char* buf){return false;}
    virtual bool setData(INDEX start, int len, void* buf) {return false;}

    virtual bool add(INDEX start, INDEX length, long long inc) { return false;}
    virtual bool add(INDEX start, INDEX length, double inc) { return false;}
    virtual void validate() {}

    virtual int compare(INDEX index, const ConstantSP& target) const {return 0;}

    virtual bool getNullFlag() const {return isNull();}
    virtual void setNullFlag(bool containNull){}
    virtual bool hasNull(){return  isNull();}
    virtual bool hasNull(INDEX start, INDEX length){return isNull();}
    virtual bool sizeable() const {return false;}
    virtual bool copyable() const {return true;}
    virtual INDEX size() const {return 1;}
    virtual INDEX itemCount() const {return getForm()==DF_MATRIX?columns():size();}
    virtual INDEX rows() const {return size();}
    virtual INDEX uncompressedRows() const {return size();}
    virtual INDEX columns() const {return 1;};
    virtual ConstantSP getMember(const ConstantSP& key) const { throw RuntimeException("getMember method not supported");}
    virtual ConstantSP keys() const {throw RuntimeException("keys method not supported");}
    virtual ConstantSP values() const {throw RuntimeException("values method not supported");}

    virtual long long releaseMemory(long long target, bool& satisfied) { satisfied = false; return 0;}
    virtual long long getAllocatedMemory() const {return 0;}
    virtual DATA_TYPE getType() const =0;
    virtual DATA_TYPE getRawType() const =0;
    virtual int getExtraParamForType() const { return 0;}
    virtual DATA_CATEGORY getCategory() const =0;

    virtual SymbolBaseSP getSymbolBase() const {return SymbolBaseSP();}
    virtual ConstantSP getInstance() const =0;
    virtual ConstantSP getValue() const =0;
    virtual OBJECT_TYPE getObjectType() const {return CONSTOBJ;}
    virtual bool isFastMode() const {return false;}
    virtual void* getDataArray() const {return nullptr;}
    virtual void** getDataSegment() const {return nullptr;}
    virtual bool isIndexArray() const { return false;}
    virtual INDEX* getIndexArray() const { return nullptr;}
    virtual bool isHugeIndexArray() const { return false;}
    virtual INDEX** getHugeIndexArray() const { return nullptr;}
    virtual int getSegmentSize() const { return 1;}
    virtual int getSegmentSizeInBit() const { return 0;}
    virtual bool containNotMarshallableObject() const {return false;}
    virtual ConstantSP castTemporal(DATA_TYPE expectType) { throw IncompatibleTypeException(getType(), expectType); }
protected:
    void checkSize(const INDEX start, const INDEX length) const
    {
        auto mySize = size();
        if (start < 0 || length < 0 || start >= mySize || length > (mySize - start)) {
			throw RuntimeException("Invalid index");
        }
    }
private:
    unsigned short flag_;

};

} // namespace dolphindb

#if defined(_MSC_VER)
#pragma warning( pop )
#elif defined(__clang__)
#pragma clang diagnostic pop
#else // gcc
#pragma GCC diagnostic pop
#endif
