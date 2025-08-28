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

		FTYPE(rb, ÂÝÎÆ¸Ö)
		FTYPE(hc, ÈÈÔþ¾í°å)
		FTYPE(bu, Ê¯ÓÍÁ¤Çà)
		FTYPE(ru, ÌìÈ»Ïð½º)
		FTYPE(br, ºÏ³ÉÏð½º)
		FTYPE(fu, È¼ÁÏÓÍ)
		FTYPE(sp, Ö½½¬)
		FTYPE(cu,Í­)
		FTYPE(al,ÂÁ)
		FTYPE(ao,Ñõ»¯ÂÁ)
		FTYPE(pb,Ç¦)
		FTYPE(zn,Ð¿)
		FTYPE(sn,Îý)
		FTYPE(ni,Äø)
		FTYPE(ss,²»Ðâ¸Ö)
		FTYPE(au,»Æ½ð)
		FTYPE(ag,°×Òø)
		FTYPE(wr,Ïß²Ä)
		FTYPE(nr,20ºÅ½º)
		FTYPE(lu,µÍÁòÈ¼ÁÏÓÍ)
		FTYPE(bc,¹ú¼ÊÍ­)
		FTYPE(sc,Ô­ÓÍ)
		FTYPE(ec,¼¯ÔËÖ¸Êý£¨Å·Ïß£©)

		FTYPE(a,»Æ´ó¶¹1ºÅ)
		FTYPE(b,»Æ´ó¶¹2ºÅ)
		FTYPE(c,ÓñÃ×)
		FTYPE(cs,ÓñÃ×µí·Û)
		FTYPE(m,¶¹ÆÉ)
		FTYPE(y,¶¹ÓÍ)
		FTYPE(p,×ØéµÓÍ)
		FTYPE(i,Ìú¿óÊ¯)
		FTYPE(j,½¹Ì¿)
		FTYPE(jm,½¹Ãº)
		FTYPE(l,¾ÛÒÒÏ©)
		FTYPE(v,¾ÛÂÈÒÒÏ©)
		FTYPE(pp,¾Û±ûÏ©)
		FTYPE(eg,ÒÒ¶þ´¼)
		FTYPE(rr,¹£Ã×)
		FTYPE(eb,±½ÒÒÏ©)
		FTYPE(pg,Òº»¯Ê¯ÓÍÆø)
		FTYPE(jd,¼¦µ°)
		FTYPE(fb,ÏËÎ¬°å)
		FTYPE(bb,½ººÏ°å)
		FTYPE(lh,ÉúÖí)
		FTYPE(lg,Ô­Ä¾)

		FTYPE(RM,²ËÆÉ)
		FTYPE(OI,²Ë×ÑÓÍ)
		FTYPE(CF,Ò»ºÅÃÞ»¨)
		FTYPE(TA,¾«¶Ô±½¶þ¼×Ëá)
		FTYPE(PX,¶Ô¶þ¼×±½)
		FTYPE(SR,°×É°ÌÇ)
		FTYPE(MA,¼×´¼)
		FTYPE(FG,²£Á§)
		FTYPE(ZC,¶¯Á¦Ãº)
		FTYPE(CY,ÃÞÉ´)
		FTYPE(SA,´¿¼î)
		FTYPE(SH,ÉÕ¼î)
		FTYPE(PF,¶ÌÏË)
		FTYPE(PR,Æ¿Æ¬)
		FTYPE(JR,¾¬µ¾)
		FTYPE(RS,²Ë×Ñ)
		FTYPE(PM,ÆÕÍ¨Ð¡Âó)
		FTYPE(WH,Ç¿Âó)
		FTYPE(RI,ÔçôÌµ¾)
		FTYPE(LR,ÍíôÌµ¾)
		FTYPE(SF,¹èÌú)
		FTYPE(SM,ÃÌ¹è)
		FTYPE(AP,Æ»¹û)
		FTYPE(CJ,ºìÔæ)
		FTYPE(UR,ÄòËØ)
		FTYPE(PK,»¨Éú)

		FTYPE(SI,¹¤Òµ¹è)
		FTYPE(LC,Ì¼Ëáï®)
		FTYPE(PS,¶à¾§¹è)

		FTYPE(IF,»¦Éî300Ö¸Êý)
		FTYPE(IH,ÉÏÖ¤50Ö¸Êý)
		FTYPE(IC,ÖÐÖ¤500Ö¸Êý)
		FTYPE(IM,ÖÐÖ¤1000Ö¸Êý)
		FTYPE(TS,2ÄêÆÚ¹úÕ®)
		FTYPE(TF,5ÄêÆÚ¹úÕ®)
		FTYPE(T,10ÄêÆÚ¹úÕ®)
		FTYPE(TL,30ÄêÆÚ¹úÕ®)

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