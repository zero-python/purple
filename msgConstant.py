#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
/**
 *源程序名称:msgConstants.py
 *软件著作权:
 *系统名称:Purple-0.1
 *模块名称:模块名
 *功能说明:定义MQ消息协议中的字段TAG
 *作   者:
 *开发日期:2019-07-20 下午2:52:29
 *版 本 号: 1.0.0.1
 *备    注: 定义MQ消息协议中的字段常量字段
 */
 """

TRADE_STATUS = {
    "START": "市场启动(初始化之后，集合竞价前)",
    "PRETR": "盘前",
    "OCALL": "开始集合竞价",
    "TRADE": "交易(连续撮合)",
    "HALT": "暂停交易",
    "SUSP": " 停盘",
    "BREAK": "休市",
    "POSMT": "科创板盘后固定价格交易",
    "POSTR": "盘后",
    "ENDTR": "交易结束",
    "STOPT": "长期停盘，停盘n天, n>=1",
    "DELISTED": "退市"}


YD_CODE = {
    "14601": "创30日新高, 每日一次",
    "14602": "创30日新低, 每日一次",
    "14603": "五分钟涨幅 >= 1%, 每日多次",
    "14604": "五分钟跌幅 <= -1%, 每日多次",
    "14605": "当日涨幅 >= 7%, 每日一次",
    "14606": "当日跌幅 <= -7%, 每日一次",
    "14607": "当日涨停, 每日一次",
    "14608": "当日跌停, 每日一次",
    "14609": "涨停开板, 每日多次",
    "14610": "跌停开盘, 每日多次"}

MARKET_CODE = {
    "XSHG.KSH": 14906,
    "XSHG.ESA.M": 14901,
    "XSHE.ESA.M": 14901,
    "XSHE.ESA.GEM": 14901,
    "XSHE.ESA.SMSE": 14901}

#
STOCK_TIMESTAMP_FORMAT = '%Y%m%d%H%M%S%f'

# provide service to node
PURPLE_NODE_ALLSTOCKINFO = "purple:node:allMarketStockInfo"
# 异动信息KEY
PURPLE_INFO = "purple:info"
# 异动落地KEY
PURPLE_SAVE = "purple:stockChange"
# 异动模块单元
ALARM_QUOTE_CHANGE: float = 7  # 涨跌幅±7%
ALARM_5MIN_QUOTE_CHANGE: float = 1  # 五分钟涨跌幅±1%
