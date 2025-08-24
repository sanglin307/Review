#include "FutureType.h"
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