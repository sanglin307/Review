#include "FutureType.h"
#include "TradeEngine.h"
#include "DolphinDBApi.h"

static std::set<std::string>  sFutureTypeSet;
static std::map<std::string, u16> sFutureTypeStrToEnum;
static std::string  sFutureTypeEnumToStr[(u16)FutureID::MAX];
static std::map<std::string, std::string>  sFutureTypeStrToName;

static std::map<std::string, Instrument> sInstrumentMap;

void InitFutureType()
{
	assert(!sFutureTypeSet.size());

#define FTYPE(E,STR) sFutureTypeSet.insert(#E);  \
                     sFutureTypeStrToEnum.insert(std::pair<std::string, u16>(#E, (u16)FutureID::E)); \
                     sFutureTypeEnumToStr[(u16)FutureID::E] = #E; \
                     sFutureTypeStrToName.insert(std::pair<std::string,std::string>(#E,#STR));

		FTYPE(rb, ���Ƹ�)
		FTYPE(hc, �������)
		FTYPE(bu, ʯ������)
		FTYPE(ru, ��Ȼ��)
		FTYPE(br, �ϳ���)
		FTYPE(fu, ȼ����)
		FTYPE(sp, ֽ��)
		FTYPE(cu,ͭ)
		FTYPE(al,��)
		FTYPE(ao,������)
		FTYPE(pb,Ǧ)
		FTYPE(zn,п)
		FTYPE(sn,��)
		FTYPE(ni,��)
		FTYPE(ss,�����)
		FTYPE(au,�ƽ�)
		FTYPE(ag,����)
		FTYPE(wr,�߲�)
		FTYPE(nr,20�Ž�)
		FTYPE(lu,����ȼ����)
		FTYPE(bc,����ͭ)
		FTYPE(sc,ԭ��)
		FTYPE(ec,����ָ����ŷ�ߣ�)

		FTYPE(a,�ƴ�1��)
		FTYPE(b,�ƴ�2��)
		FTYPE(c,����)
		FTYPE(cs,���׵��)
		FTYPE(m,����)
		FTYPE(y,����)
		FTYPE(p,�����)
		FTYPE(i,����ʯ)
		FTYPE(j,��̿)
		FTYPE(jm,��ú)
		FTYPE(l,����ϩ)
		FTYPE(v,������ϩ)
		FTYPE(pp,�۱�ϩ)
		FTYPE(eg,�Ҷ���)
		FTYPE(rr,����)
		FTYPE(eb,����ϩ)
		FTYPE(pg,Һ��ʯ����)
		FTYPE(jd,����)
		FTYPE(fb,��ά��)
		FTYPE(bb,���ϰ�)
		FTYPE(lh,����)
		FTYPE(lg,ԭľ)

		FTYPE(RM,����)
		FTYPE(OI,������)
		FTYPE(CF,һ���޻�)
		FTYPE(TA,���Ա�������)
		FTYPE(PX,�Զ��ױ�)
		FTYPE(SR,��ɰ��)
		FTYPE(MA,�״�)
		FTYPE(FG,����)
		FTYPE(ZC,����ú)
		FTYPE(CY,��ɴ)
		FTYPE(SA,����)
		FTYPE(SH,�ռ�)
		FTYPE(PF,����)
		FTYPE(PR,ƿƬ)
		FTYPE(JR,����)
		FTYPE(RS,����)
		FTYPE(PM,��ͨС��)
		FTYPE(WH,ǿ��)
		FTYPE(RI,���̵�)
		FTYPE(LR,���̵�)
		FTYPE(SF,����)
		FTYPE(SM,�̹�)
		FTYPE(AP,ƻ��)
		FTYPE(CJ,����)
		FTYPE(UR,����)
		FTYPE(PK,����)

		FTYPE(SI,��ҵ��)
		FTYPE(LC,̼���)
		FTYPE(PS,�ྦྷ��)

		FTYPE(IF,����300ָ��)
		FTYPE(IH,��֤50ָ��)
		FTYPE(IC,��֤500ָ��)
		FTYPE(IM,��֤1000ָ��)
		FTYPE(TS,2���ڹ�ծ)
		FTYPE(TF,5���ڹ�ծ)
		FTYPE(T,10���ڹ�ծ)
		FTYPE(TL,30���ڹ�ծ)

#undef FUTURE_TYPE

}

TradeEngine& TradeEngine::Instance()
{
	static TradeEngine Inst;
	return Inst;
}

void TradeEngine::Init()
{
	InitFutureType();
	DolphinDBApi::Instance().Init();
	DolphinDBApi::Instance().GetInstrumentTable(sInstrumentMap);
}

void TradeEngine::Destroy()
{
	DolphinDBApi::Instance().Destroy();
}

FutureID TradeEngine::ParseFutureID(const std::string& str)
{
	auto iter = sFutureTypeStrToEnum.find(str);
	assert(iter != sFutureTypeStrToEnum.end());
	return FutureID(iter->second);
}

const std::string& TradeEngine::ToFutureIDStr(const FutureID id)
{
	assert(id < FutureID::MAX);
	return sFutureTypeEnumToStr[u16(id)];
}