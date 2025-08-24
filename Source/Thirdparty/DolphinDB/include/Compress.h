// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Types.h"
#include "SysIO.h"
#include "Vector.h"

namespace dolphindb {

class CompressEncoderDecoder;
using CompressEncoderDecoderSP = SmartPointer<CompressEncoderDecoder>;

class CompressionFactory {
public:
#pragma pack (1)
	struct Header {
		int byteSize;
		int colCount;
		char version;
		char flag;
		char charCode;
		char compressedType;
		char dataType;
		char unitLength;
		short reserved;
		int extra;
		int elementCount;
		int checkSum;
	};
#pragma pack ()
	static CompressEncoderDecoderSP GetEncodeDecoder(COMPRESS_METHOD type);
	static IO_ERR decode(const DataInputStreamSP& compressSrc, DataOutputStreamSP &uncompressResult, Header &header);
	static IO_ERR encodeContent(const VectorSP &vec, const DataOutputStreamSP &compressResult, Header &header, bool checkSum);
};

class CompressEncoderDecoder {
public:
	virtual IO_ERR decode(DataInputStreamSP compressSrc, DataOutputStreamSP &uncompressResult, const CompressionFactory::Header &header) = 0;
	virtual IO_ERR encodeContent(const VectorSP &vec, const DataOutputStreamSP &compressResult, CompressionFactory::Header &header, bool checkSum) = 0;
	virtual ~CompressEncoderDecoder() = default;
};

class CompressLZ4 : public CompressEncoderDecoder {
public:
	IO_ERR decode(DataInputStreamSP compressSrc, DataOutputStreamSP &uncompressResult, const CompressionFactory::Header &header) override;
	IO_ERR encodeContent(const VectorSP &vec, const DataOutputStreamSP &compressResult, CompressionFactory::Header &header, bool checkSum) override;
	~CompressLZ4() override;
private:
	char * newBuffer(int size);
	std::vector<char*> tempBufList_;
};

class CompressDeltaofDelta : public CompressEncoderDecoder {
public:
	IO_ERR decode(DataInputStreamSP compressSrc, DataOutputStreamSP &uncompressResult, const CompressionFactory::Header &header) override;
	IO_ERR encodeContent(const VectorSP &vec, const DataOutputStreamSP &compressResult, CompressionFactory::Header &header, bool checkSum) override;
	~CompressDeltaofDelta() override;
private:
	char * newBuffer(int size);
	std::vector<char*> tempBufList_;
	static const int maxDecompressedSize_;
	static const int maxCompressedSize_;
};

} // namespace dolphindb
