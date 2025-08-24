#pragma once

#include <set>
#include <string>
#include <map>
#include <vector>
#include <cassert>

typedef char  i8;
typedef short i16;
typedef int   i32;
typedef long long i64;

typedef unsigned char u8;
typedef unsigned short u16;
typedef unsigned int u32;
typedef unsigned int uint;
typedef unsigned long long u64;

enum class FutureID : u16
{
	None = 0,
	// SHFE  
	rb,        // 螺纹钢
	hc,        // 热轧卷板
	bu,        // 石油沥青
	ru,        // 天然橡胶
	br,        // 合成橡胶
	fu,        // 燃料油
	sp,        // 纸浆
	cu,        // 铜
	al,        // 铝
	ao,        // 氧化铝
	pb,        // 铅
	zn,        // 锌
	sn,        // 锡
	ni,        // 镍
	ss,        // 不锈钢
	au,        // 黄金
	ag,        // 白银
	wr,        // 线材

	// INE
	nr = 50,   //20号胶
	lu,        //低硫燃料油
	bc,        // 国际铜
	sc,        // 原油
	ec,        // 集运指数（欧线）

	// DCE
	a = 100,   // 黄大豆1号
	b,         // 黄大豆2号
	c,         // 玉米
	cs,        // 玉米淀粉
	m,         // 豆粕
	y,         // 豆油
	p,         // 棕榈油
	i,         // 铁矿石
	j,         // 焦炭
	jm,        // 焦煤
	l,         // 聚乙烯
	v,         // 聚氯乙烯
	pp,        // 聚丙烯
	eg,        // 乙二醇
	rr,        // 梗米
	eb,        // 苯乙烯
	pg,        // 液化石油气
	jd,        // 鸡蛋
	fb,        // 纤维板
	bb,        // 胶合板
	lh,        // 生猪
	lg,        // 原木

	// CZCE
	RM = 150,  // 菜粕
	OI,        // 菜籽油
	CF,        // 一号棉花
	TA,        // 精对苯二甲酸
	PX,        // 对二甲苯
	SR,        // 白砂糖
	MA,        // 甲醇
	FG,        // 玻璃
	ZC,        // 动力煤
	CY,        // 棉纱
	SA,        // 纯碱
	SH,        // 烧碱
	PF,        // 短纤
	PR,        // 瓶片
	JR,        // 粳稻
	RS,        // 菜籽
	PM,        // 普通小麦
	WH,        // 强麦
	RI,        // 早籼稻
	LR,        // 晚籼稻
	SF,        // 硅铁
	SM,        // 锰硅
	AP,        // 苹果
	CJ,        // 红枣
	UR,        // 尿素
	PK,        // 花生

	// GFEX
	SI = 200,  // 工业硅
	LC,        // 碳酸锂
	PS,        // 多晶硅

	// CFFEX
	IF = 250,  //沪深300指数
	IH,        //上证50指数
	IC,        //中证500指数
	IM,        //中证1000指数
	TS,        //2年期国债
	TF,        //5年期国债
	T,         //10年期国债
	TL,        //30年期国债

	MAX
};
 