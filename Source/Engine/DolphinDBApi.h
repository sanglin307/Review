#pragma once

#include "DolphinDB.h"
#include "Util.h"

class DolphinDBApi
{
public:
	static DolphinDBApi& Instance();

	void Init();
	void Destroy();

	void GetInstrumentTable(std::map<std::string, Instrument>& instrumentMap);
	void InsertInstrumentTable(std::vector<Instrument>& inst);

protected:
	DolphinDBApi() = default;
	DolphinDBApi(const DolphinDBApi& rhs) = delete;
	DolphinDBApi(DolphinDBApi&& rhs) = delete;
	DolphinDBApi& operator=(const DolphinDBApi& rhs) = delete;
	DolphinDBApi& operator=(DolphinDBApi&& rhs) = delete;

private:
	dolphindb::DBConnection  _connection;

};