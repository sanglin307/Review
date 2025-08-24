// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Exports.h"
#include <atomic>

namespace dolphindb {

class Counter {
public:
	explicit Counter(void* p): p_(p), count_(0){}
	int addRef(){ return atomic_fetch_add(&count_,1)+1;} //atomic operation
	int release(){return atomic_fetch_sub(&count_,1)-1;} //atomic operation
	int getCount() const {return count_.load();}
	void* p_;

private:
	std::atomic<int> count_;
};


template <class T>
class SmartPointer {
public:
	SmartPointer(T* p=0): counterP_(new Counter(p)){ // NOLINT(google-explicit-constructor, hicpp-explicit-conversions)
		counterP_->addRef();
	}

	SmartPointer(const SmartPointer<T>& sp){
		counterP_=sp.counterP_;
		counterP_->addRef();
	}

	template <class U>
	SmartPointer(const SmartPointer<U>& sp){ // NOLINT(google-explicit-constructor, hicpp-explicit-conversions)
		counterP_=sp.counterP_;
		counterP_->addRef();
	}

	T& operator *() const{
		return *((T*)counterP_->p_);
	}

	T* operator ->() const{
		return (T*)counterP_->p_;
	}

	SmartPointer<T>& operator=(const SmartPointer<T>& sp)
	{
		if(this==&sp)
			return *this;

		Counter* tmp = sp.counterP_;
		tmp->addRef();

		Counter* oldCounter = counterP_;
		counterP_= tmp;

		if(oldCounter->release()==0){
			delete (T*)oldCounter->p_;
			delete oldCounter;
		}
		return *this;
	}

	bool operator ==(const SmartPointer<T>& sp) const{
		return counterP_ == sp.counterP_;
	}

	bool operator !=(const SmartPointer<T>& sp) const{
		return counterP_ != sp.counterP_;
	}

	void clear(){
		auto* tmp = new Counter(nullptr);
		tmp->addRef();

		Counter* oldCounter = counterP_;
		counterP_= tmp;

		if(oldCounter->release()==0){
			delete (T*)oldCounter->p_;
			delete oldCounter;
		}
	}

	bool isNull() const{
		return counterP_->p_ == nullptr;
	}

	int count() const{
		return counterP_->getCount();
	}

	T* get() const{
		return (T*)counterP_->p_;
	}

	~SmartPointer(){
		if(counterP_->release()==0){
			delete static_cast<T*>(counterP_->p_);
			delete counterP_;
			counterP_=nullptr;
		}
	}
private:
	template<class U> friend class SmartPointer;
	Counter* counterP_;
};

} // namespace dolphindb
