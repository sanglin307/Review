// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

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

#include "Constant.h"
#include "SmartPointer.h"

namespace dolphindb {

class EXPORT_DECL Vector:public Constant{
public:
    Vector(): Constant(259){}
    ~Vector() override = default;
    ConstantSP getColumnLabel() const override;
    const std::string& getName() const {return name_;}
    void setName(const std::string& name){name_=name;}
    bool isLargeConstant() const override { return isMatrix() || size()>1024; }
    virtual void initialize(){}
    virtual INDEX getCapacity() const = 0;
    virtual	INDEX reserve(INDEX capacity) {throw RuntimeException("Vector::reserve method not supported");}
    virtual	void resize(INDEX sz) {throw RuntimeException("Vector::resize method not supported");}
    virtual INDEX getValueSize() const {throw RuntimeException("Vector::getValueSize method not supported");}
    virtual size_t getUnitLength() const = 0;
    virtual void clear()=0;
    virtual bool remove(INDEX count){return false;}
    virtual bool remove(const ConstantSP& index){return false;}
    virtual bool append(const ConstantSP& value){return append(value, value->size());}
    virtual bool append(const ConstantSP& value, INDEX count){return false;}
    virtual bool append(const ConstantSP& value, INDEX start, INDEX length){return false;}
    virtual bool appendBool(char* buf, int len){return false;}
    virtual bool appendChar(char* buf, int len){return false;}
    virtual bool appendShort(short* buf, int len){return false;}
    virtual bool appendInt(int* buf, int len){return false;}
    virtual bool appendLong(long long* buf, int len){return false;}
    virtual bool appendIndex(INDEX* buf, int len){return false;}
    virtual bool appendFloat(float* buf, int len){return false;}
    virtual bool appendDouble(double* buf, int len){return false;}
    virtual bool appendString(std::string* buf, int len){return false;}
    virtual bool appendString(char** buf, int len){return false;}
    std::string getString() const override;
    std::string getScript() const override;
    std::string getString(INDEX index) const override = 0;
    virtual VECTOR_TYPE getVectorType() const{return VECTOR_TYPE::ARRAY;}
    virtual bool isSorted(bool asc, bool strict = false) const {throw RuntimeException("Vector::isSorted method not supported");}
    ConstantSP getInstance() const override {return getInstance(size());}
    virtual ConstantSP getInstance(INDEX size) const = 0;
    using Constant::getValue;
    virtual ConstantSP getValue(INDEX capacity) const {throw RuntimeException("Vector::getValue method not supported");}
    virtual ConstantSP get(INDEX column, INDEX rowStart,INDEX rowEnd) const {return getSubVector((column*rows())+rowStart,rowEnd-rowStart);}
    ConstantSP get(INDEX index) const override = 0;
    ConstantSP getWindow(INDEX colStart, int colLength, INDEX rowStart, int rowLength) const override {return getSubVector(rowStart,rowLength);}
    virtual ConstantSP getSubVector(INDEX start, INDEX length) const { throw RuntimeException("getSubVector method not supported");}
    virtual ConstantSP getSubVector(INDEX start, INDEX length, INDEX capacity) const { return getSubVector(start, length);}
    virtual void fill(INDEX start, INDEX length, const ConstantSP& value)=0;
    virtual void next(INDEX steps)=0;
    virtual void prev(INDEX steps)=0;
    virtual void reverse()=0;
    virtual void reverse(INDEX start, INDEX length)=0;
    virtual void replace(const ConstantSP& oldVal, const ConstantSP& newVal){}
    virtual bool validIndex(INDEX uplimit){return false;}
    virtual bool validIndex(INDEX start, INDEX length, INDEX uplimit){return false;}
    virtual void addIndex(INDEX start, INDEX length, INDEX offset){}
    virtual void neg()=0;
    virtual void upper(){throw RuntimeException("upper method not supported");}
    virtual void lower(){throw RuntimeException("lower method not supported");}
    virtual void trim(){throw RuntimeException("trim method not supported");}
    virtual void strip(){throw RuntimeException("strip method not supported");}
    using Constant::getAllocatedMemory;
    virtual long long getAllocatedMemory(INDEX sz) const {return Constant::getAllocatedMemory();}
    virtual int asof(const ConstantSP& value) const {throw RuntimeException("asof not supported.");}
    ConstantSP castTemporal(DATA_TYPE expectType) override{throw RuntimeException("castTemporal not supported");}

protected:
    using Constant::get;

private:
    std::string name_;
};
using VectorSP = SmartPointer<Vector>;
} // namespace dolphindb

#if defined(_MSC_VER)
#pragma warning( pop )
#elif defined(__clang__)
#pragma clang diagnostic pop
#else // gcc
#pragma GCC diagnostic pop
#endif
