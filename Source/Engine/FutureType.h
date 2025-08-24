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
	rb,        // ���Ƹ�
	hc,        // �������
	bu,        // ʯ������
	ru,        // ��Ȼ��
	br,        // �ϳ���
	fu,        // ȼ����
	sp,        // ֽ��
	cu,        // ͭ
	al,        // ��
	ao,        // ������
	pb,        // Ǧ
	zn,        // п
	sn,        // ��
	ni,        // ��
	ss,        // �����
	au,        // �ƽ�
	ag,        // ����
	wr,        // �߲�

	// INE
	nr = 50,   //20�Ž�
	lu,        //����ȼ����
	bc,        // ����ͭ
	sc,        // ԭ��
	ec,        // ����ָ����ŷ�ߣ�

	// DCE
	a = 100,   // �ƴ�1��
	b,         // �ƴ�2��
	c,         // ����
	cs,        // ���׵��
	m,         // ����
	y,         // ����
	p,         // �����
	i,         // ����ʯ
	j,         // ��̿
	jm,        // ��ú
	l,         // ����ϩ
	v,         // ������ϩ
	pp,        // �۱�ϩ
	eg,        // �Ҷ���
	rr,        // ����
	eb,        // ����ϩ
	pg,        // Һ��ʯ����
	jd,        // ����
	fb,        // ��ά��
	bb,        // ���ϰ�
	lh,        // ����
	lg,        // ԭľ

	// CZCE
	RM = 150,  // ����
	OI,        // ������
	CF,        // һ���޻�
	TA,        // ���Ա�������
	PX,        // �Զ��ױ�
	SR,        // ��ɰ��
	MA,        // �״�
	FG,        // ����
	ZC,        // ����ú
	CY,        // ��ɴ
	SA,        // ����
	SH,        // �ռ�
	PF,        // ����
	PR,        // ƿƬ
	JR,        // ����
	RS,        // ����
	PM,        // ��ͨС��
	WH,        // ǿ��
	RI,        // ���̵�
	LR,        // ���̵�
	SF,        // ����
	SM,        // �̹�
	AP,        // ƻ��
	CJ,        // ����
	UR,        // ����
	PK,        // ����

	// GFEX
	SI = 200,  // ��ҵ��
	LC,        // ̼���
	PS,        // �ྦྷ��

	// CFFEX
	IF = 250,  //����300ָ��
	IH,        //��֤50ָ��
	IC,        //��֤500ָ��
	IM,        //��֤1000ָ��
	TS,        //2���ڹ�ծ
	TF,        //5���ڹ�ծ
	T,         //10���ڹ�ծ
	TL,        //30���ڹ�ծ

	MAX
};
 