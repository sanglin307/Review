#include "FutureType.h"
#include "TradeEngine.h"
#include "DolphinDBApi.h"

using namespace dolphindb;

DolphinDBApi& DolphinDBApi::Instance()
{
    static DolphinDBApi Inst;
    return Inst;
}

void DolphinDBApi::Init()
{
    bool ret = _connection.connect("127.0.0.1", 8848,"admin","123456");
    if (!ret) {
        assert(0);
        return;
    }
}

void DolphinDBApi::Destroy()
{
    _connection.close();
}

void DolphinDBApi::GetInstrumentTable(std::map<std::string, Instrument>& instrumentMap)
{
    TableSP result = _connection.run("SELECT * FROM loadTable(\"dfs://working_data\", \"instrument\")");
    for (int i = 0; i < result->size(); i++)
    {
        DictionarySP c = result->get(i);
        int days = c->getMember("Date")->getInt();
        int y, m, d;
        Util::parseDate(days, y, m, d);
        Instrument Inst = {
            .ID = c->getMember("ID")->getStringRef(),
            .IsContinue = c->getMember("Continue")->getBool() > 0 ? true : false,
            .Type = TradeEngine::Instance().ParseFutureID(c->getMember("Type")->getStringRef()),
            .ExpirationDate = std::chrono::year_month(std::chrono::year(y), std::chrono::month(m))
        };

        instrumentMap.insert(std::pair<std::string, Instrument>(Inst.ID, Inst));
    }
}

void DolphinDBApi::InsertInstrumentTable(std::vector<Instrument>& inst)
{
    std::vector<std::string> colNames = { "ID", "Continue", "Type", "Date" };
    std::vector<DATA_TYPE> colTypes = { DT_SYMBOL, DT_BOOL, DT_SYMBOL, DT_DATE};
    ConstantSP table = Util::createTable(colNames, colTypes, (int)inst.size(), (int)inst.size());
    std::vector<VectorSP> columnVecs;
    for (int i = 0; i < 4; i++)
        columnVecs.push_back(table->getColumn(i));

    for (int i = 0; i < inst.size(); i++) 
    {
        columnVecs[0]->setString(i, inst[i].ID);
        columnVecs[1]->setBool(i, inst[i].IsContinue);
        columnVecs[2]->setString(i, TradeEngine::Instance().ToFutureIDStr(inst[i].Type));
        columnVecs[3]->setInt(i, Util::countDays(int(inst[i].ExpirationDate.year()), (unsigned int)(inst[i].ExpirationDate.month()), 15));
    }

    std::vector<ConstantSP> args;
    args.push_back(table);
    _connection.run("tableInsert{loadTable('dfs://working_data', `instrument)}", args);
}