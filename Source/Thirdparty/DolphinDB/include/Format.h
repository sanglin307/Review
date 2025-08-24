// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include <string>
#include <vector>

#include "Types.h"

namespace dolphindb {

class TemporalFormat {
public:
    explicit TemporalFormat(const std::string& format);
    std::string format(long long nowtime, DATA_TYPE dtype) const;
    static std::vector<std::pair<int, int> > initFormatMap();

private:
    void initialize(const std::string& format);


    struct FormatSegment {
        int timeUnitIndex_;
        int maxLength_;
        int startPos_;
        int endPos_;

        FormatSegment() : timeUnitIndex_(0), maxLength_(0), startPos_(0), endPos_(0) {}
        FormatSegment(int timeUnitIndex, int maxLength, int startPos, int endPos)
            : timeUnitIndex_(timeUnitIndex), maxLength_(maxLength), startPos_(startPos), endPos_(endPos) {}
    };

    std::string format_;
    bool quickFormat_;
    int segmentCount_;
    FormatSegment segments_[12];

    static std::vector<std::pair<int, int> > formatMap;  // first:timeUnitIndex_ second:timeUnit max length
    static const std::string pmString;
    static const std::string amString;
    static const char* monthName[12];
};

class NumberFormat {
public:
    explicit NumberFormat(const std::string& format);
    std::string format(double x) const;
    static std::string toString(long long x);

private:
	 void initialize(const std::string& format);
	 static int printFraction(char* buf, int digitCount, bool optional, double& fraction);


    bool percent_;
    bool point_;
    int science_;
    int segmentLength_;
    int integerMinDigits_;
    int fractionMinDigits_;
    int fractionOptionalDigits_;
    int headSize_;
    int tailSize_;
    std::string head_;
	std::string tail_;
	double rounding_;

	static const long long power10_[10];
	static const double enableScientificNotationBeyond_;
	static const double epsilon_;
};

class DecimalFormat {
public:
	explicit DecimalFormat(const std::string& format);
	~DecimalFormat();
	std::string format(double x) const;

private:
	NumberFormat* format_;
	NumberFormat* negFormat_;
};

} // namespace dolphindb
