#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
每天收盘从Wind同步个股简况数据到Redis.db03: stkRealTimeState
    shsc                    : 沪港通 true or false AShareDescription.IS_SHSC
    smt                     : 融资融券 1 or 0 asharemargintrade 中国A股融资融券交易明细
    ksh                     : 科创板   1 or 0 AShareDescription.S_INFO_LISTBOARD == '434009000'
    company_name            : 公司名称-简称 AShareDescription.S_INFO_COMPNAME
    ipoDate                 : 股票IPO时间，time.time  AShareIPO.s_ipo_listdate 上市日 float(time.mktime(datetime.datetime.strptime('20010827', '%Y%m%d').timetuple()))
    pubPrice                : 发行价格 AShareIPO.s_ipo_price 发行价格
    pubAmt                  : 发行数量（股本）AShareIPO.s_ipo_amount 发行数量 + 万股
    pb                      : 市净率 ashareeodderivativeindicator.S_VAL_PB_NEW
    pe                      : 市盈率 ashareeodderivativeindicator.S_VAL_PE
    ps                      : 市销率 ashareeodderivativeindicator.S_VAL_PS
    tfc                     : 流通市值 ashareeodderivativeindicator.S_DQ_MV (万) * 10000 (元)
    mktc                    : 总市值 ashareeodderivativeindicator.S_VAL_MV (万) * 10000 (元)
    total                   : 总股本（股）ashareeodderivativeindicator.TOT_SHR_TODAY (万) * 10000 (股)
    ashares_trade           : A股流通股本 ashareeodderivativeindicator.FLOAT_A_SHR_TODAY (万) * 10000 (股)
    city                    : 股票公司所在城市 AShareIntroduction.S_INFO_CITY
    legrep                  : 法人代表 AShareIntroduction.S_INFO_CHAIRMAN
    officeaddr              : 办公地址 AShareIntroduction.s_info_office
    introduction            : 公司中文简介 AShareIntroduction.s_info_ChineseIntroduction
    regcapital              : 注册资本（万元）AShareIntroduction.s_info_regcapital + 万元
    income                  : 收入构成  [{"cb":"total_revenue","lv":45543434000}, 总收入   -- AShareIncome.TOT_OPER_REV
                                        {"cb":"operation_profit","lv":2898894000}, 营业利润 -- AShareIncome.OPER_PROFIT
                                        {"cb":"net_profits", "lv":2244526000}] 净利润    -- AShareIncome.NET_PROFIT_EXCL_MIN_INT_INC
    bonus                   : 添加分股股票数据 -- ashareexrightdividendrecord(中国A股除权除息记录) ex_type = "分红"
                                    ['bonusyear': i.bonus_year, 除权除息日 ashareexrightdividendrecord.ex_date 除权除息日
                                    'exrightdate': str(i.ex_dt)[:10], ashareexrightdividendrecord.ex_description 除权说明
                                    'summarize': i.memo] ashareexrightdividendrecord.ex_description 除权说明
    dailyData_saveTime      : 同步时间 -- YYYY-MM-DD HH:MM:SS
    shrNm                   : 股票名称 AShareDescription.S_INFO_NAME
日行情 -- 每日17:00更新
"""
import time
import datetime
import numpy as np
from quotations.util.common import timeit, tradeit
from quotations.manager.mysqlManager import MysqlManager
from quotations.manager.redisManager import RedisManager
from quotations.plate.purple.compat import is_redispy2, is_redispy3
from quotations.plate.purple import utils as PurpleUtils
import json


# Chyi add
engine_wind = MysqlManager('wind').engine
redisManager3 = RedisManager('bus_3')


def fetch_stock_list(if_refresh):
    """
    获取需要更新的股票列表
    :param if_refresh:
    :return:
    """
    with MysqlManager('wind') as session:
        try:
            if if_refresh:
                # asharedescription: 中国A股基本资料；ashareipo: 中国A股首次公开发行数据表
                sql_fetch_all = """SELECT asharedescription.S_INFO_WINDCODE
                FROM asharedescription, ashareipo 
                WHERE asharedescription.S_INFO_WINDCODE = ashareipo.S_INFO_WINDCODE 
                AND asharedescription.S_INFO_LISTDATE IS NOT NULL"""
                basics = session.read_sql(sql_fetch_all, to_DataFrame=True)
            else:
                sql_update = """SELECT asharedescription.S_INFO_WINDCODE
                            FROM asharedescription, ashareipo 
                            WHERE asharedescription.S_INFO_WINDCODE = ashareipo.S_INFO_WINDCODE 
                            AND asharedescription.S_INFO_LISTDATE IS NOT NULL 
                            AND asharedescription.S_INFO_DELISTDATE IS NULL"""
                basics = session.read_sql(sql_update, to_DataFrame=True)
            return basics['S_INFO_WINDCODE'].tolist()
        except Exception as e:
            print(e)
        finally:
            print()


def fetch_exright_dividend_record(lstock):
    """
    获取中国A股除权除息记录: 更新字段: bonus
    :param lstock: 股票列表
    :return: 更新对应的redis数据
    """
    with MysqlManager('wind') as session:
        try:
            strstock = str(lstock).replace('[', '(').replace(']', ')')
            # ashareexrightdividendrecord: 中国A股除权除息记录
            sql_fetch_all = """SELECT S_INFO_WINDCODE, EX_DATE, EX_DESCRIPTION 
            FROM ashareexrightdividendrecord 
            WHERE EX_TYPE = '分红' AND S_INFO_WINDCODE IN {} ORDER BY EX_DATE DESC""".format(strstock)
            rst_df = session.read_sql(sql_fetch_all, to_DataFrame=True)
            rst_df.dropna(axis=0, how='all', inplace=True)
            print("获取{}条数据".format(len(rst_df)))
            # Group by S_INFO_WINDCODE
            for code, __df in rst_df.groupby(['S_INFO_WINDCODE']):
                bonus = list()  # 初始化
                for index, row in __df.iterrows():  # 遍历所有行
                    # 判断每一行中"EX_DESCRIPTION"出现多少个[yyyymmdd]
                    raw_info = row['EX_DESCRIPTION'].split('[')
                    raw_info = list(filter(None, raw_info))
                    if ',' in raw_info:
                        raw_info.remove(',')
                    if ';' in raw_info:
                        raw_info.remove(';')
                    for info in raw_info:
                        if info[:8].isdigit():  # --Wind库中各种奇葩的数据
                            bonus.append({
                                "bonusyear": PurpleUtils.dateFormatConvert(info[:8],
                                                                           sformat='%Y%m%d',
                                                                           dformat='%Y-%m-%d'),
                                "exrightdate": PurpleUtils.dateFormatConvert(row['EX_DATE'],
                                                                             sformat='%Y%m%d',
                                                                             dformat='%Y-%m-%d'),
                                "summarize": info[9:]
                            })
                # 是否需要过滤科创板
                if code[:3] == '688':
                    code_type = '14906'
                else:
                    code_type = '14901'
                code_no_suffix = code[:6]
                redisManager3.hmset('stkRealTimeState:{}_{}'.format(code_no_suffix, code_type),
                                    mapping={'bonus': json.dumps(bonus),  # 'bonus': str(bonus),
                                             'timeShareDaily_saveTime': datetime.datetime.now().strftime('%Y%m%d%H%M%S')})
        except Exception as e:
            print("error: {}".format(e))
        finally:
            print('fetch_exright_dividend_record run OVER')


def fetch_asharedescription(lstock):
    """
    中国A股基本资料: 更新字段 shrNm, company_name, shsc, ksh
    :param lstock: 股票列表
    :return: 更新对应的redis数据
    """
    with MysqlManager('wind') as session:
        try:
            strstock = str(lstock).replace('[', '(').replace(']', ')')
            # ashareexrightdividendrecord: 中国A股除权除息记录
            sql_fetch_all = """SELECT S_INFO_WINDCODE, S_INFO_NAME, S_INFO_COMPNAME, IS_SHSC, S_INFO_LISTBOARD
            FROM asharedescription 
            WHERE S_INFO_WINDCODE IN {}""".format(strstock)
            rst_df = session.read_sql(sql_fetch_all, to_DataFrame=True)
            rst_df.dropna(axis=0, how='all', inplace=True)
            rst_df['IS_SHSC'] = rst_df['IS_SHSC'].astype('int32')
            rst_df.drop_duplicates(subset=['S_INFO_WINDCODE'], keep='first', inplace=True)
            print("中国A股除权除息记录获取{}条数据".format(len(rst_df)))
            pipe = redisManager3.pipeline()
            for index, row in rst_df.iterrows():  # 遍历所有行
                # 是否需要过滤科创板
                if row['S_INFO_LISTBOARD'] == '434009000':  # 上市板类型
                    code_type = '14906'
                else:
                    code_type = '14901'
                code_no_suffix = row['S_INFO_WINDCODE'][:6]
                pipe.hmset('stkRealTimeState:{}_{}'.format(code_no_suffix, code_type),
                           mapping={'shsc': row['IS_SHSC'],
                                    'ksh': 1 if code_type == '14906' else 0,
                                    'shrNm': row['S_INFO_NAME'].strip(),
                                    'company_name': row['S_INFO_COMPNAME'].strip(),
                                    'timeShareDaily_saveTime': datetime.datetime.now().strftime('%Y%m%d%H%M%S')})
                if index % 200 == 0 or index == len(rst_df)-1:
                    pipe.execute()
            pipe.execute()
        except Exception as e:
            print("error: {}".format(e))
        finally:
            print('fetch_asharedescription run OVER')


def fetch_asharemargintrade(lstock):
    """
    中国A股融资融券: 更新字段 smt
    :param lstock: 股票列表
    :return: 更新对应的redis数据
    """
    with MysqlManager('wind') as session:
        try:
            # asharemargintrade: 中国A股融资融券交易
            sql_fetch_all = """SELECT S_INFO_WINDCODE 
            FROM asharemargintrade 
            WHERE TRADE_DT = (SELECT TRADE_DT FROM asharemargintrade ORDER BY TRADE_DT DESC LIMIT 1)"""
            rst_df = session.read_sql(sql_fetch_all, to_DataFrame=True)
            rst_df.dropna(axis=0, how='all', inplace=True)
            rst_df.drop_duplicates(subset=['S_INFO_WINDCODE'], keep='first', inplace=True)
            print("中国A股融资融券交易获取{}条数据".format(len(rst_df)))
            pipe = redisManager3.pipeline()
            for index, code in enumerate(rst_df['S_INFO_WINDCODE'].tolist()):  # 遍历所有融资融券
                # 是否需要过滤科创板
                if code[:3] == '688':
                    code_type = '14906'
                else:
                    code_type = '14901'
                code_no_suffix = code[:6]
                pipe.hmset('stkRealTimeState:{}_{}'.format(code_no_suffix, code_type),
                           mapping={'smt': 1,
                                    'timeShareDaily_saveTime': datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                                    })
                if index % 200 == 0 or index == len(rst_df['S_INFO_WINDCODE'].tolist())-1:
                    pipe.execute()
            pipe.execute()
            for index, code in enumerate(set(lstock) - set(rst_df['S_INFO_WINDCODE'].tolist())):  # 遍历所有非融资融券
                # 是否需要过滤科创板
                if code[:3] == '688':
                    code_type = '14906'
                else:
                    code_type = '14901'
                code_no_suffix = code[:6]
                pipe.hmset('stkRealTimeState:{}_{}'.format(code_no_suffix, code_type),
                           mapping={'smt': 0,
                                    'timeShareDaily_saveTime': datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                                    })
                if index % 200 == 0 or index == len(set(lstock) - set(rst_df['S_INFO_WINDCODE'].tolist()))-1:
                    pipe.execute()
            pipe.execute()
        except Exception as e:
            print("error: {}".format(e))
        finally:
            print('fetch_asharemargintrade run OVER')


def fetch_ashareipo(lstock):
    """
    中国A股首次公开发行数据: 更新字段 ipoDate, pubPrice, pubAmt
    :param lstock: 股票列表
    :return: 更新对应的redis数据
    """
    with MysqlManager('wind') as session:
        try:
            # ashareipo: 中国A股首次公开发行数据
            sql_fetch_all = """SELECT S_INFO_WINDCODE, S_IPO_LISTDATE, S_IPO_PRICE, S_IPO_AMOUNT FROM ashareipo"""
            rst_df = session.read_sql(sql_fetch_all, to_DataFrame=True)
            rst_df.dropna(axis=0, how='any', inplace=True)
            rst_df.drop_duplicates(subset=['S_INFO_WINDCODE'], keep='first', inplace=True)
            print("获取{}条数据".format(len(rst_df)))
            pipe = redisManager3.pipeline()
            for index, row in rst_df.iterrows():  # 遍历所有行
                # 是否需要过滤科创板
                if row['S_INFO_WINDCODE'][:3] == '688':
                    code_type = '14906'
                else:
                    code_type = '14901'
                code_no_suffix = row['S_INFO_WINDCODE'][:6]
                pipe.hmset('stkRealTimeState:{}_{}'.format(code_no_suffix, code_type),
                           mapping={'ipoDate': PurpleUtils.dateFormatConvert(row['S_IPO_LISTDATE'],
                                                                             '%Y%m%d', '%Y-%m-%d'),
                                    'pubPrice': row['S_IPO_PRICE'],  # 发行价
                                    'pubAmt': str(row['S_IPO_AMOUNT']) + '万股',  # -- 万，亿更改
                                    'timeShareDaily_saveTime': datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                                    })
                if index % 200 == 0 or index == len(rst_df)-1:
                    pipe.execute()
            pipe.execute()
        except Exception as e:
            print("error: {}".format(e))
        finally:
            print('fetch_ashareipo run OVER')


def fetch_ashareeodderivativeindicator(lstock):
    """
    中国A股日行情估值指标: 更新字段 pb, pe, ps, tfc, mktc, total, ashares_trade
    :param lstock: 股票列表
    :return: 更新对应的redis数据
    """
    with MysqlManager('wind') as session:
        try:
            # ashareeodderivativeindicator: 中国A股日行情估值指标
            sql_fetch_all = """SELECT S_INFO_WINDCODE, S_VAL_PB_NEW, S_VAL_PE, S_VAL_PS, S_DQ_MV,
            S_VAL_MV, TOT_SHR_TODAY, FLOAT_A_SHR_TODAY
            FROM ashareeodderivativeindicator
            WHERE TRADE_DT = (SELECT TRADE_DT FROM ashareeodderivativeindicator ORDER BY TRADE_DT DESC LIMIT 1)"""
            rst_df = session.read_sql(sql_fetch_all, to_DataFrame=True)
            rst_df.dropna(axis=0, how='all', inplace=True)
            rst_df.drop_duplicates(subset=['S_INFO_WINDCODE'], keep='first', inplace=True)
            print("中国A股日行情估值指标 获取{}条数据".format(len(rst_df)))
            pipe = redisManager3.pipeline()
            for index, row in rst_df.iterrows():  # 遍历所有行
                # 是否需要过滤科创板
                if row['S_INFO_WINDCODE'][:3] == '688':
                    code_type = '14906'
                else:
                    code_type = '14901'
                code_no_suffix = row['S_INFO_WINDCODE'][:6]
                pipe.hmset('stkRealTimeState:{}_{}'.format(code_no_suffix, code_type),
                           mapping={'pb': row['S_VAL_PB_NEW'] if not np.isnan(row['S_VAL_PB_NEW']) else '',   # 市净率
                                    'pe': row['S_VAL_PE'] if not np.isnan(row['S_VAL_PE']) else '',  # 市盈率
                                    'ps': row['S_VAL_PS'] if not np.isnan(row['S_VAL_PS']) else '',  # 市销率
                                    'tfc': row['S_DQ_MV'] * 10000,  # 流通市值
                                    'mktc': row['S_VAL_MV'] * 10000,  # 总市值
                                    'total': int(row['TOT_SHR_TODAY'] * 10000),  # 总股本（股
                                    'ashares_trade': int(row['FLOAT_A_SHR_TODAY'] * 10000),  # A股流通股本
                                    'timeShareDaily_saveTime': datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                                    })
                if index % 200 == 0 or index == len(rst_df) - 1:
                    pipe.execute()
            pipe.execute()
            #
        except Exception as e:
            print("error: {}".format(e))
        finally:
            print('fetch_ashareeodderivativeindicator run OVER')


def fetch_ashareintroduction(lstock):
    """
    中国A股公司简介: 更新字段city, legrep, officeaddr, introduction, regcapital
    :param lstock: 股票列表
    :return: 更新对应的redis数据
    """
    with MysqlManager('wind') as session:
        try:
            strstock = str(lstock).replace('[', '(').replace(']', ')')
            # ashareeodderivativeindicator: 中国A股日行情估值指标
            sql_fetch_all = """SELECT S_INFO_WINDCODE, S_INFO_CITY, S_INFO_CHAIRMAN, S_INFO_OFFICE,
            S_INFO_CHINESEINTRODUCTION, S_INFO_REGCAPITAL
            FROM ashareintroduction
            WHERE S_INFO_WINDCODE in {}""".format(strstock)
            rst_df = session.read_sql(sql_fetch_all, to_DataFrame=True)
            rst_df.dropna(axis=0, how='all', inplace=True)
            rst_df.drop_duplicates(subset=['S_INFO_WINDCODE'], keep='first', inplace=True)
            print("中国A股公司简介获取{}条数据".format(len(rst_df)))
            pipe = redisManager3.pipeline()
            for index, row in rst_df.iterrows():  # 遍历所有行
                # 是否需要过滤科创板
                if row['S_INFO_WINDCODE'][:3] == '688':
                    code_type = '14906'
                else:
                    code_type = '14901'
                code_no_suffix = row['S_INFO_WINDCODE'][:6]
                pipe.hmset('stkRealTimeState:{}_{}'.format(code_no_suffix, code_type),
                           mapping={'city': row['S_INFO_CITY'].strip() if row['S_INFO_CITY'] is not None else '',  # 股票公司所在城市
                                    'legrep': row['S_INFO_CHAIRMAN'].strip() if row['S_INFO_CHAIRMAN'] is not None else '',  # 法人代表
                                    'officeaddr': row['S_INFO_OFFICE'].strip() if row['S_INFO_OFFICE'] is not None else '',  # 办公地址
                                    'introduction': row['S_INFO_CHINESEINTRODUCTION'].strip() if row['S_INFO_CHINESEINTRODUCTION'] is not None else '',  # 公司中文简介
                                    'regcapital': str(row['S_INFO_REGCAPITAL'])+'万元' if row['S_INFO_REGCAPITAL'] is not None else '',  # 注册资本（万元）
                                    'timeShareDaily_saveTime': datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                                    })
                if index % 200 == 0 or index == len(rst_df) - 1:
                    pipe.execute()
            pipe.execute()
        except Exception as e:
            print("error: {}".format(e))
        finally:
            print('fetch_ashareeodderivativeindicator run OVER')


def fetch_ashareincome(lstock):
    """
    中国A股利润表: 更新字段 income
    :param lstock: 股票列表
    :return: 更新对应的redis数据
    """
    with MysqlManager('wind') as session:
        try:
            strstock = str(lstock).replace('[', '(').replace(']', ')')
            # ashareincome: 中国A股利润表
            sql_fetch_all = """SELECT T.S_INFO_WINDCODE, T.ANN_DT, T.TOT_OPER_REV, T.OPER_PROFIT, T.NET_PROFIT_EXCL_MIN_INT_INC
            FROM 
            (
            SELECT S_INFO_WINDCODE, ANN_DT, TOT_OPER_REV, OPER_PROFIT, NET_PROFIT_EXCL_MIN_INT_INC 
            FROM ashareincome
            WHERE S_INFO_WINDCODE in {} ORDER BY ANN_DT DESC
            ) AS T
            GROUP BY T.S_INFO_WINDCODE""".format(strstock)
            rst_df = session.read_sql(sql_fetch_all, to_DataFrame=True)
            rst_df.dropna(axis=0, how='all', inplace=True)
            rst_df.drop_duplicates(subset=['S_INFO_WINDCODE'], keep='first', inplace=True)
            print("中国A股利润表{}条数据".format(len(rst_df)))
            pipe = redisManager3.pipeline()
            for index, row in rst_df.iterrows():  # 遍历所有行
                # 是否需要过滤科创板
                if row['S_INFO_WINDCODE'][:3] == '688':
                    code_type = '14906'
                else:
                    code_type = '14901'
                income = list()  # 初始化
                for k, v in zip(['total_revenue', 'operation_profit', 'net_profits'], ['TOT_OPER_REV', 'OPER_PROFIT', 'NET_PROFIT_EXCL_MIN_INT_INC']):
                    income.append({
                        "cb": k,
                        "lv": row[v] if not np.isnan(row[v]) else ''})
                code_no_suffix = row['S_INFO_WINDCODE'][:6]
                pipe.hmset('stkRealTimeState:{}_{}'.format(code_no_suffix, code_type),
                           mapping={'income': json.dumps(income),
                                    'timeShareDaily_saveTime': datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                                    })
                if index % 200 == 0 or index == len(rst_df) - 1:
                    pipe.execute()
            pipe.execute()
        except Exception as e:
            print("error: {}".format(e))
        finally:
            print('fetch_ashareincome run OVER')


def fetch_planB(lstock):
    """
    中国A股利润表: 更新字段 income
    :param lstock: 股票列表
    :return: 更新对应的redis数据
    """
    with MysqlManager('wind') as session:
        try:
            strstock = str(lstock).replace('[', '(').replace(']', ')')
            # ashareincome: 中国A股利润表
            sql_fetch_all = """SELECT T.S_INFO_WINDCODE, T.ANN_DT, T.TOT_OPER_REV, T.OPER_PROFIT, T.NET_PROFIT_EXCL_MIN_INT_INC
            FROM 
            (
            SELECT S_INFO_WINDCODE, ANN_DT, TOT_OPER_REV, OPER_PROFIT, NET_PROFIT_EXCL_MIN_INT_INC 
            FROM ashareincome
            WHERE S_INFO_WINDCODE in {} ORDER BY ANN_DT DESC
            ) AS T
            GROUP BY T.S_INFO_WINDCODE""".format(strstock)
            rst_df = session.read_sql(sql_fetch_all, to_DataFrame=True)
            rst_df.dropna(axis=0, how='all', inplace=True)
            rst_df.drop_duplicates(subset=['S_INFO_WINDCODE'], keep='first', inplace=True)
            print("中国A股利润表{}条数据".format(len(rst_df)))
            pipe = redisManager3.pipeline()
            for index, row in rst_df.iterrows():  # 遍历所有行
                # 是否需要过滤科创板
                if row['S_INFO_WINDCODE'][:3] == '688':
                    code_type = '14906'
                else:
                    code_type = '14901'
                income = list()  # 初始化
                for k, v in zip(['total_revenue', 'operation_profit', 'net_profits'], ['TOT_OPER_REV', 'OPER_PROFIT', 'NET_PROFIT_EXCL_MIN_INT_INC']):
                    income.append({
                        "cb": k,
                        "lv": row[v] if not np.isnan(row[v]) else ''})
                code_no_suffix = row['S_INFO_WINDCODE'][:6]
                pipe.hmset('stkRealTimeState:{}_{}'.format(code_no_suffix, code_type),
                           mapping={'income': json.dumps(income),
                                    'timeShareDaily_saveTime': datetime.datetime.now().strftime('%Y%m%d%H%M%S')
                                    })
                if index % 200 == 0 or index == len(rst_df) - 1:
                    pipe.execute()
            pipe.execute()
        except Exception as e:
            print("error: {}".format(e))
        finally:
            print('fetch_ashareincome run OVER')


@tradeit
@timeit
def run(if_refresh=False):
    """
    1. 选择是否全量覆盖或日度更新
    :param if_refresh: 是否是全量覆盖更新True, False
    :return:
    """
    lstock = fetch_stock_list(if_refresh)
    print("今日 [{}] 更新股票数: {}".format('全量' if if_refresh else '日度', len(lstock)))

    # 获取中国A股除权除息记录
    print("获取中国A股除权除息记录, 更新字段bonus")
    fetch_exright_dividend_record(lstock)

    # 获取中国A股基本资料
    print("获取中国A股基本资料, 更新shrNm, company_name, shsc, ksh")
    fetch_asharedescription(lstock)

    # # 获取中国A股融资融券交易
    print("获取中国A股融资融券交易明细，更新字段smt")
    fetch_asharemargintrade(lstock)

    # 获取中国A股首次公开发行数据
    print("获取中国A股首次发行数据, 更新字段ipoDate, pubPrice, pubAmt")
    fetch_ashareipo(lstock)

    # 获取A股日行情估值指标
    print("获取中国A股行情估值指标, 更新字段pb, pe, ps, tfc, mktc, total, ashares_trade")
    fetch_ashareeodderivativeindicator(lstock)

    # 获取中国A股公司简介
    print("获取中国A股公司简介，更新字段city, legrep, officeaddr, introduction, regcapital")
    fetch_ashareintroduction(lstock)

    # 获取当日新股首次发行的市盈率和市净率 --
    # print("获取中国A股首次发行数据，覆盖更新字段pe [s_ipo_dilutedpe]")

    # 获取股票收入构成
    print("获取中国A股利润表，覆盖更新字段income")
    fetch_ashareincome(lstock)

    # Execution Plan B
    print("Executing Plan B")
    fetch_planB(lstock)


if __name__ == '__main__':
    run(if_refresh=False)
