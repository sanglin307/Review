#include "FutureType.h"
#include "TradeEngine.h"
#include "DolphinDBApi.h"

static std::set<std::string>  sFutureTypeSet;
static std::map<std::string, u16> sFutureTypeStrToEnum;
static std::string  sFutureTypeEnumToStr[(u16)FutureID::MAX];
static std::map<std::string, std::string>  sFutureTypeStrToName;

void InitFutureType()
{
	assert(!sFutureTypeSet.size());

#define FTYPE(E,STR) sFutureTypeSet.insert(#E);  \
                     sFutureTypeStrToEnum.insert(std::pair<std::string, u16>(#E, (u16)FutureID::E)); \
                     sFutureTypeEnumToStr[(u16)FutureID::E] = #E; \
                     sFutureTypeStrToName.insert(std::pair<std::string,std::string>(#E,#STR));

		FTYPE(rb, 螺纹钢)
		FTYPE(hc, 热轧卷板)
		FTYPE(bu, 石油沥青)
		FTYPE(ru, 天然橡胶)
		FTYPE(br, 合成橡胶)
		FTYPE(fu, 燃料油)
		FTYPE(sp, 纸浆)
		FTYPE(cu,铜)
		FTYPE(al,铝)
		FTYPE(ao,氧化铝)
		FTYPE(pb,铅)
		FTYPE(zn,锌)
		FTYPE(sn,锡)
		FTYPE(ni,镍)
		FTYPE(ss,不锈钢)
		FTYPE(au,黄金)
		FTYPE(ag,白银)
		FTYPE(wr,线材)
		FTYPE(nr,20号胶)
		FTYPE(lu,低硫燃料油)
		FTYPE(bc,国际铜)
		FTYPE(sc,原油)
		FTYPE(ec,集运指数（欧线）)

		FTYPE(a,黄大豆1号)
		FTYPE(b,黄大豆2号)
		FTYPE(c,玉米)
		FTYPE(cs,玉米淀粉)
		FTYPE(m,豆粕)
		FTYPE(y,豆油)
		FTYPE(p,棕榈油)
		FTYPE(i,铁矿石)
		FTYPE(j,焦炭)
		FTYPE(jm,焦煤)
		FTYPE(l,聚乙烯)
		FTYPE(v,聚氯乙烯)
		FTYPE(pp,聚丙烯)
		FTYPE(eg,乙二醇)
		FTYPE(rr,梗米)
		FTYPE(eb,苯乙烯)
		FTYPE(pg,液化石油气)
		FTYPE(jd,鸡蛋)
		FTYPE(fb,纤维板)
		FTYPE(bb,胶合板)
		FTYPE(lh,生猪)
		FTYPE(lg,原木)

		FTYPE(RM,菜粕)
		FTYPE(OI,菜籽油)
		FTYPE(CF,一号棉花)
		FTYPE(TA,精对苯二甲酸)
		FTYPE(PX,对二甲苯)
		FTYPE(SR,白砂糖)
		FTYPE(MA,甲醇)
		FTYPE(FG,玻璃)
		FTYPE(ZC,动力煤)
		FTYPE(CY,棉纱)
		FTYPE(SA,纯碱)
		FTYPE(SH,烧碱)
		FTYPE(PF,短纤)
		FTYPE(PR,瓶片)
		FTYPE(JR,粳稻)
		FTYPE(RS,菜籽)
		FTYPE(PM,普通小麦)
		FTYPE(WH,强麦)
		FTYPE(RI,早籼稻)
		FTYPE(LR,晚籼稻)
		FTYPE(SF,硅铁)
		FTYPE(SM,锰硅)
		FTYPE(AP,苹果)
		FTYPE(CJ,红枣)
		FTYPE(UR,尿素)
		FTYPE(PK,花生)

		FTYPE(SI,工业硅)
		FTYPE(LC,碳酸锂)
		FTYPE(PS,多晶硅)

		FTYPE(IF,沪深300指数)
		FTYPE(IH,上证50指数)
		FTYPE(IC,中证500指数)
		FTYPE(IM,中证1000指数)
		FTYPE(TS,2年期国债)
		FTYPE(TF,5年期国债)
		FTYPE(T,10年期国债)
		FTYPE(TL,30年期国债)

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

}

void TradeEngine::Destroy()
{
	DolphinDBApi::Instance().Destroy();
}