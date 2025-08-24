#include "EventHandler.h"
#include "Constant.h"
#include "ConstantImp.h"
#include "ConstantMarshall.h"
#include "DolphinDB.h"
#include "ErrorCodeInfo.h"
#include "Exceptions.h"
#include "SysIO.h"
#include "Table.h"
#include "Types.h"
#include "Util.h"
#include "Vector.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <string>
#include <unordered_map>
#include <vector>

namespace dolphindb {

IO_ERR AttributeSerializer::serialize(const ConstantSP& attribute, const DataOutputStreamSP &outStream){
    ConstantMarshallSP marshal;
    if(form_ == DF_SCALAR || attribute->getForm() == form_){
        marshal = ConstantMarshallFactory::getInstance(attribute->getForm(), outStream);
    }
    else{
        return INVALIDDATA;
    }
    IO_ERR ret;
    marshal->start(attribute, true, false, ret);
    return ret;
}

IO_ERR FastArrayAttributeSerializer::serialize(const ConstantSP &attribute, const DataOutputStreamSP &outStream)
{
    int curCount = attribute->size();
    if (curCount == 0) {
        auto tCnt = static_cast<short>(curCount);
        outStream->write((char*)&tCnt, sizeof(short));
        return OK;
    }

    unsigned short arrayRows = 1;
    unsigned char curCountBytes = 1;
    char reserved = 0;
    int maxCount = 255;
    while(curCountBytes < 4 && curCount > maxCount){
        curCountBytes *= 2;
        maxCount = (1ULL << (8U * curCountBytes)) - 1;
    }
    outStream->write((char*)&arrayRows, sizeof(unsigned short));
    outStream->write((char*)&curCountBytes, sizeof(unsigned char));
    outStream->write(&reserved, sizeof(char));
    switch (curCountBytes) {
        case 1 : {
            auto tCnt = static_cast<unsigned char>(curCount);
            outStream->write((char*)&tCnt, sizeof(unsigned char));
            break;
        }
        case 2 : {
            auto tCnt = static_cast<unsigned short>(curCount);
            outStream->write((char*)&tCnt, sizeof(unsigned short));
            break;
        }
        default : {
            auto tCnt = static_cast<unsigned int>(curCount);
            outStream->write((char*)&tCnt, sizeof(unsigned int));
        }
    }
    void* pData = attribute->getDataArray();
    outStream->write((char *)pData, curCount * unitLen_);
    return IO_ERR::OK;
}

IO_ERR ScalarAttributeSerializer::serialize(const ConstantSP &attribute, const DataOutputStreamSP &outStream)
{
    int numElement, partial;
    attribute->serialize(&buf_[0], unitLen_, 0, 0, numElement, partial);
    if(numElement != 1 ) return INVALIDDATA;
    outStream->write(buf_.data(), unitLen_);
    return IO_ERR::OK;
}

IO_ERR StringScalarAttributeSerializer::serialize(const ConstantSP &attribute, const DataOutputStreamSP &outStream)
{
    const std::string& buf = attribute->getStringRef();
    if(isBlob_){
        size_t len = buf.size();
        outStream->write((char *)&len, sizeof(int));
        outStream->write(buf.data(), len);
    }
    else{
        outStream->write(buf.data(), buf.size() + 1);
    }
    return IO_ERR::OK;
}

ConstantSP EventHandler::deserializeScalar(DATA_TYPE type, int extraParam, const DataInputStreamSP& input, IO_ERR& ret){
    int numElement = 0;
    ConstantSP data = Util::createConstant(type, extraParam);
    ret = data->deserialize(input.get(), 0, 1, numElement);
    if(ret != IO_ERR::OK) return nullptr;
    return data;
}

ConstantSP EventHandler::deserializeFastArray(DATA_TYPE type, int extraParam, const DataInputStreamSP& input, IO_ERR& ret){
    int numElement = 0;
    short count = 0;
    ret = input->peekBuffer((char*)&count, sizeof(short));
    if(ret != OK) return nullptr;
    if(count == 0){
        input->readShort(count);
        return Util::createVector(type, 0, 0, true, extraParam);
    }
    ConstantSP data = Util::createVector(static_cast<DATA_TYPE>(type + ARRAY_TYPE_BASE), 1, 1, true, extraParam);
    ret = data->deserialize(input.get(), 0, 1, numElement);
    if (ret != OK) return nullptr;
    return ((FastArrayVector*)data.get())->getSourceValue();
}

ConstantSP EventHandler::deserializeAny(DATA_TYPE type, DATA_FORM form, const DataInputStreamSP& input, IO_ERR& ret){
    uint16_t flag;
    ret = input->readShort(flag);
    if (ret != OK) 
        return nullptr;
    auto readForm = static_cast<DATA_FORM>(flag >> 8U);
    if(type != DT_ANY && form != readForm){
        ret = INVALIDDATA;
        return nullptr;
    }
    ConstantUnmarshallSP unmarshall = ConstantUnmarshallFactory::getInstance(readForm, input);
    if (UNLIKELY(unmarshall == nullptr)) {
        ret = INVALIDDATA;
        return nullptr;
    }
    if (!unmarshall->start(flag, true, ret)) {
        return nullptr;
    }
    return unmarshall->getConstant();
}

bool EventHandler::checkSchema(const std::vector<EventSchema>& eventSchemas, const std::vector<std::string> &expandTimeKeys, const std::vector<std::string>& commonFields, std::string& errMsg){
    int index = 0;
    for(const auto& schema : eventSchemas){
        if(eventInfos_.count(schema.eventType_) != 0){
            errMsg = "EventType must be unique";
            return false;
        }

        EventSchemaExSP schemaEx(new EventSchemaEx);
        schemaEx->schema_ = schema;

        if(isNeedEventTime_){
            auto iter = std::find(schema.fieldNames_.begin(), schema.fieldNames_.end(), expandTimeKeys[index]);
            if(iter == schema.fieldNames_.end()){
                errMsg = "Event " + schema.eventType_ + " doesn't contain eventTimeField " + expandTimeKeys[index];
                return false;
            }
            schemaEx->timeIndex_ = static_cast<int>(std::distance(schema.fieldNames_.begin(), iter));
        }

        for(const auto& commonField : commonFields){
            auto iter = std::find(schema.fieldNames_.begin(), schema.fieldNames_.end(), commonField);
            if(iter == schema.fieldNames_.end()){
                errMsg = "Event " + schema.eventType_ + " doesn't contain commonField " + commonField;
                return false;
            }
            schemaEx->commonFieldIndex_.push_back(static_cast<int>(std::distance(schema.fieldNames_.begin(), iter)));
        }

        std::vector<AttributeSerializerSP> serls;
        size_t length = schema.fieldNames_.size();
        for(size_t j = 0; j < length; ++j){
            DATA_TYPE type = schema.fieldTypes_[j];
            DATA_FORM form = schema.fieldForms_[j];
            if(form < 0 || form >= DATA_FORM::MAX_DATA_FORM){
                errMsg = "Invalid data form for the field " + schema.fieldNames_[j] + " of event " + schema.eventType_;
                return false;
            }

            if(type < 0 || type > DATA_TYPE::DT_OBJECT_ARRAY){
                errMsg = "Invalid data type for the field " + schema.fieldNames_[j] + " of event " + schema.eventType_;
                return false;
            }

            DATA_TYPE actualType = type > 64 ? static_cast<DATA_TYPE>(type - 64) : type;
            if(Util::getCategory(actualType) == DENARY){
                //check extra Param
                ConstantSP tempDecimal = Util::createConstant(actualType, schema.fieldExtraParams_[j]);
            }

            if((form == DF_SCALAR || form == DF_VECTOR) && type < ARRAY_TYPE_BASE && type != DT_ANY){
                int unitLen = Util::getDataTypeSize(type);
                if(type == DT_SYMBOL){
                    //the size of symbol is 4, but it need to be serialized as a string
                    unitLen = -1;
                }
                if(unitLen > 0){
                    if(form == DF_SCALAR){
                        serls.push_back(new ScalarAttributeSerializer(unitLen));
                    }
                    else{
                        serls.push_back(new FastArrayAttributeSerializer(unitLen));
                    }
                    continue;
                }
                //BLOB STRING
                if(unitLen < 0 && form != DF_VECTOR){
                    serls.push_back(new StringScalarAttributeSerializer(type == DT_BLOB));
                    continue;
                }
            }
            serls.push_back(new AttributeSerializer(0, form));
        }

        EventInfo info{serls, schemaEx};
        eventInfos_[schema.eventType_] = info;
        index++;
    }
    return true;
}

EventHandler::EventHandler(const std::vector<EventSchema>& eventSchemas, const std::vector<std::string>& eventTimeFields, const std::vector<std::string>& commonFields)
    : isNeedEventTime_(false), outputColNums_(0), commonFieldSize_(0)
{
    std::string funcName = "createEventSender";
    //check eventSchemas
    if(eventSchemas.empty()){
        throw IllegalArgumentException(funcName, "eventSchemas must not be empty");
    }
    std::vector<EventSchema> expandEventSchemas = eventSchemas;
    for(auto& event : expandEventSchemas){
        if(event.eventType_.empty()){
            throw IllegalArgumentException(funcName, "eventType must not be empty.");
        }
        size_t length = event.fieldNames_.size();
        if(event.fieldExtraParams_.empty()){
            event.fieldExtraParams_.resize(length, 0);
        }
        if(length != event.fieldTypes_.size() || length != event.fieldForms_.size() || length != event.fieldExtraParams_.size()){
            throw IllegalArgumentException(funcName, "Vectors in EventSchema must be of the same length.");
        }
        if(length == 0){
            throw IllegalArgumentException(funcName, "Empty EventSchema in eventSchemas.");
        }
    }
    size_t eventNum = eventSchemas.size();

    //check eventTimeFields
    std::vector<std::string> expandTimeKeys;
    if(!eventTimeFields.empty()){
        //if eventTimeFields only contain one element, it means every event has this key
        if(eventTimeFields.size() == 1){
            expandTimeKeys.resize(eventNum, eventTimeFields[0]);
        }
        else{
            if(eventTimeFields.size() != eventNum){
                throw IllegalArgumentException(funcName, "The number of eventTimeField is inconsistent with the number of events in eventSchemas.");
            }
            expandTimeKeys = eventTimeFields;
        }
        isNeedEventTime_ = true;
    }

    //prepare eventInfos_
    std::string errMsg;
    if(!checkSchema(expandEventSchemas, expandTimeKeys, commonFields, errMsg)){
        throw IllegalArgumentException(funcName, errMsg);
    }
    commonFieldSize_ = static_cast<int>(commonFields.size());
}

bool EventHandler::deserializeEvent(const ConstantSP& obj, std::vector<std::string>& eventTypes, std::vector<std::vector<ConstantSP>>& attributes, ErrorCodeInfo& errorInfo){
    int eventTypeIndex = isNeedEventTime_ ? 1 : 0;
    int blobIndex = isNeedEventTime_ ? 2 : 1;
    const auto eventTypeVec = obj->get(eventTypeIndex);
    const auto blobVec = obj->get(blobIndex);
    int rowSize = eventTypeVec->size();
    IO_ERR ioError;
    for(int rowIndex = 0; rowIndex < rowSize; ++rowIndex){
        std::string eventType = eventTypeVec->getString(rowIndex);
        auto iter = eventInfos_.find(eventType);
        if(iter == eventInfos_.end()){
            errorInfo.set(ErrorCodeInfo::EC_InvalidParameter, "UnKnown eventType" + eventType);
            return false;
        }
        const std::string &blob = blobVec->getStringRef(rowIndex);
        DataInputStreamSP input = new DataInputStream(blob.data(), blob.size(), false);

        EventSchema& schema = iter->second.eventSchema_->schema_;
        size_t attrCount = schema.fieldTypes_.size();
        std::vector<ConstantSP> datas(attrCount);
        for(size_t i = 0; i < attrCount; ++i){
            DATA_FORM form = schema.fieldForms_[i];
            DATA_TYPE type = schema.fieldTypes_[i];
            int extraParam = schema.fieldExtraParams_[i];
            if(form == DF_SCALAR){
                if(type == DT_ANY){
                    datas[i] = deserializeAny(type, form, input, ioError);
                }else{
                    datas[i] = deserializeScalar(type, extraParam, input, ioError);
                }
            } else if(form == DF_VECTOR){
                if(type < ARRAY_TYPE_BASE && Util::getCategory(type) != LITERAL){
                    datas[i] = deserializeFastArray(type, extraParam, input, ioError);
                } else{
                    datas[i] = deserializeAny(type, form, input, ioError);
                }
            } else{
                datas[i] = deserializeAny(type, form, input, ioError);
            }
            if(datas[i].isNull()){
                errorInfo.set(ErrorCodeInfo::EC_InvalidObject, "Deserialize blob error " + std::to_string(ioError));
                return false;
            }
        }
        eventTypes.push_back(eventType);
        attributes.push_back(datas);
    }
    return true;
}

bool EventHandler::serializeEvent(const std::string& eventType, const std::vector<ConstantSP>& attributes, std::vector<ConstantSP>& serializedEvent, std::string& errMsg){
    auto iter = eventInfos_.find(eventType);
    if(iter == eventInfos_.end()){
        errMsg = "unknown eventType " + eventType;
        return false;
    }
    const EventInfo& info = iter->second;
    if(attributes.size() != info.attributeSerializers_.size()){
        errMsg = "the number of event values does not match " + eventType;
        return false;
    }

    for(unsigned i = 0; i < attributes.size(); ++i){
        if(info.eventSchema_->schema_.fieldTypes_[i] != attributes[i]->getType()){
            //An exception: when the type in schema is symbol, you can pass a string attribute
            if(info.eventSchema_->schema_.fieldTypes_[i] == DT_SYMBOL && attributes[i]->getType() == DT_STRING){
                continue;
            }
            errMsg = "Expected type for the field " + info.eventSchema_->schema_.fieldNames_[i] + " of " + eventType + " : " + Util::getDataTypeString(info.eventSchema_->schema_.fieldTypes_[i]) +
            ", but now it is " + Util::getDataTypeString(attributes[i]->getType());
            return false;
        }
        if(info.eventSchema_->schema_.fieldForms_[i] != attributes[i]->getForm()){
            errMsg = "Expected form for the field " + info.eventSchema_->schema_.fieldNames_[i] + " of " + eventType + " : " + Util::getDataFormString(info.eventSchema_->schema_.fieldForms_[i]) +
            ", but now it is " + Util::getDataFormString(attributes[i]->getForm());
            return false;
        }
        if(Util::getCategory(attributes[i]->getRawType()) == DENARY){
            if(info.eventSchema_->schema_.fieldExtraParams_[i] != attributes[i]->getExtraParamForType()){
                errMsg = "Expected extraParams for the field " + info.eventSchema_->schema_.fieldNames_[i] + " of " + eventType + " : " + std::to_string(info.eventSchema_->schema_.fieldExtraParams_[i]) +
                ", but now it is " + std::to_string(attributes[i]->getExtraParamForType());
                return false;
            }
        }
    }

    //std::vector<ConstantSP> oneLineContent;
    VectorSP oneLineContent = Util::createVector(dolphindb::DT_ANY, 0, outputColNums_);
    if(isNeedEventTime_){
        oneLineContent->append(attributes[info.eventSchema_->timeIndex_]);
    }
    oneLineContent->append(Util::createString(eventType));
    //serialize all attribute to a blob
    DataOutputStreamSP outStream = new DataOutputStream(1024);
    for(unsigned i = 0; i < attributes.size(); ++i){
        IO_ERR ret = info.attributeSerializers_[i]->serialize(attributes[i], outStream);
        if(ret != IO_ERR::OK){
            errMsg = "Failed to serialize the field " + info.eventSchema_->schema_.fieldNames_[i] + ", errorCode " + std::to_string(ret);
            return false;
        }
    }
    oneLineContent->append(Util::createBlob(std::string(outStream->getBuffer(), outStream->size())));

    for(auto commonIndex : info.eventSchema_->commonFieldIndex_){
        if(info.eventSchema_->schema_.fieldForms_[commonIndex] == DF_VECTOR){
            VectorSP any = Util::createVector(DT_ANY, 0, 0);
            any->append(attributes[commonIndex]);
            oneLineContent->append((ConstantSP)any);
        }
        else{
            oneLineContent->append(attributes[commonIndex]);
        }
    }
    serializedEvent.emplace_back(oneLineContent);
    return true;
}

bool EventHandler::checkOutputTable(const TableSP& outputTable, std::string& errMsg){
    outputColNums_ = isNeedEventTime_ ? (3 + commonFieldSize_) : (2 + commonFieldSize_);
    if(outputColNums_ != outputTable->columns()){
        errMsg = "Incompatible outputTable columnns, expected: " + std::to_string(outputColNums_) + ", got: " + std::to_string(outputTable->columns());
        return false;
    }
    int colIdx = 0;
    if(isNeedEventTime_){
        if(Util::getCategory(outputTable->getColumnType(0)) != TEMPORAL){
            errMsg = "The first column of the output table must be temporal if eventTimeField is specified.";
            return false;
        }
        colIdx++;
    }
    int typeIdx_ = colIdx++;
    int blobIdx_ = colIdx++;

    if(outputTable->getColumnType(typeIdx_) != DT_STRING && outputTable->getColumnType(typeIdx_) != DT_SYMBOL){
        errMsg = "The eventType column of the output table must be STRING or SYMBOL type.";
        return false;
    }
    if(outputTable->getColumnType(blobIdx_) != DT_BLOB){
        errMsg = "The event column of the output table must be BLOB type.";
        return false;
    }

    return true;
}


EventSender::EventSender(const DBConnectionSP& conn, const std::string& tableName, const std::vector<EventSchema>& eventSchema, const std::vector<std::string>& eventTimeFields, const std::vector<std::string>& commonFields)
    : eventHandler_(eventSchema, eventTimeFields, commonFields), conn_(conn)
{
    if(tableName.empty()){
        throw RuntimeException("tableName must not be empty.");
    }
    std::string sql = "select top 0 * from " + tableName;
    std::string errMsg;
    ConstantSP outputTable = conn_->run(sql);
    if(!eventHandler_.checkOutputTable((TableSP)outputTable, errMsg)){
        throw RuntimeException(errMsg);
    }
    insertScript_ = "tableInsert{" + tableName + "}";
}

void EventSender::sendEvent(const std::string& eventType, const std::vector<ConstantSP>& attributes){
    std::vector<ConstantSP> args;
    std::string errMsg;
    if(!eventHandler_.serializeEvent(eventType, attributes, args, errMsg)){
        throw RuntimeException("serialize event Fail for " + errMsg);
    }
    conn_->run(insertScript_, args);
}

} // namespace dolphindb
