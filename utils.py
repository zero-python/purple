# -*- coding:utf-8 -*-
"""
purple.utils
~~~~~~~~~~~~

This module provides utility functions that are used within purple
that are also useful for external consumption.
:copyright: (c) 2019 by zero
:license:
"""
# itertools : Functions creating iterators for efficient looping¶
from itertools import zip_longest
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import json
import datetime
import os
import signal
from operator import itemgetter
from quotations.plate.purple import msgConstant as CONSTANT

BaseModel = declarative_base()


def batcher(iterable, n):
    """
    iterate a list in batches of size n
    :param iterable:
    :param n:
    :return:
    """
    args = [iter(iterable)] * n
    return zip_longest(*args)


def create_table(model, engine, param=None):
    """创建ORM表对象"""
    tables = dict()
    if param:
        table_name = '{}_{}'.format(model.__tag__, param)
    else:
        table_name = model.__tag__
    if not tables.get(table_name):
        ifexists = False  # 表是否存在
        tables[table_name] = type(table_name, (model, BaseModel), {'__tablename__': table_name})
        # 表不存在时执行以下语句
        # TODO: 增加验证表是否存在的语句, SHOW TABLES LIKE '%tablename%';
        with engine.connect() as con:
            rs = con.execute('SHOW TABLES LIKE "{}"'.format(table_name)).fetchone()
            ifexists = True if rs else False
        if not ifexists:
            try:
                print("create table: {}".format(table_name))
                tables[table_name].__table__.create(bind=engine)
            except Exception as e:
                raise Exception(e)
        else:
            print("table: {} already exists".format(table_name))
    return tables[table_name]


def json2df(ljson):
    """
    转换JsonString to DataFrame
    :param ljson: list json string
    :return: DataFrame
    """
    if isinstance(ljson, list):
        ldict = list(map(lambda x: json.loads(x), ljson))
        return pd.DataFrame(ldict)
    elif isinstance(ljson, str):
        return pd.DataFrame(json.loads(ljson))
    else:
        print("Only input String, or List")
        return None


def msg2dict(msg):
    """
    rabbitMQ Message convert to dict
    :param msg: bytes
    :return: dict
    """
    if isinstance(msg, bytes):
        return json.loads(msg.decode('UTF-8'))
    else:
        print("Only input bytes, or bytearray")
        return None


def dict2json(msg):
    """
    rabbitMQ Message convert to dict
    :param msg: dict
    :return: str object
    """
    if isinstance(msg, dict):
        # json.dumps() converts a dictionary to str object
        return json.dumps(msg)
    else:
        print("Only input dict")
        return None


def start_status():
    """
    股票交易状态 -- 市场启动(初始化之后，集合竞价前)
    :return:
    """
    return False


def pretr_status():
    """
    股票交易状态 -- 盘前
    :return:
    """
    return False


def ocall_status():
    """
    股票交易状态 -- 开始集合竞价
    :return:
    """
    return False


def trade_status():
    """
    股票交易状态 -- 交易连续撮合
    :return:
    """
    return True


def half_status():
    """
    股票交易状态 -- 盘中临时暂停交易
    :return:
    """
    return True


def susp_status():
    """
    股票交易状态 -- 停盘
    :return:
    """
    return True


def break_status():
    """
    股票交易状态 -- 休市
    :return:
    """
    return False


def posmt_status():
    """
    股票交易状态 -- 科创板盘后固定价格交易
    :return:
    """
    return False


def postr_status():
    """
    股票交易状态 -- 盘后
    :return:
    """
    return False


def endtr_status():
    """
    股票交易状态 -- 交易结束
    :return:
    """
    return False


def stopt_status():
    """
    股票交易 -- 长期停盘,停盘n天,n>=1
    :return:
    """
    return False


def delisted_status():
    """
    股票交易 -- 退市
    :return:
    """
    return False


def check_stock_status(status):
    """
    查看股票状态
    :param status: str
    :return: Boolean True|False
    """
    switcher = {
        "START": start_status,          # 初始化之后，集合竞价之前
        "PRETR": pretr_status,          # 盘前
        "OCALL": ocall_status,          # 开始集合竞价
        "TRADE": trade_status,          # 交易，连续撮合
        "HALF": half_status,            # 盘中临时停盘
        "SUSP": susp_status,            # 盘中停盘
        "BREAK": break_status,          # 休市
        "POSMT": posmt_status,          # 科创板盘后固定价格交易
        "POSTR": postr_status,          # 盘后
        "ENDTR": endtr_status,          # 交易结束
        "STOPT": stopt_status,          # 长期停盘
        "DELISTED": delisted_status     # 退市
    }
    # Get the function from switcher dictionary
    func = switcher.get(status, lambda: "Invalid Stock Status")
    # Execute the function
    return func()


def df2dict(df, key=None):
    """
    转换DataFrame为Python dict对象
    :param df: pd.DataFrame
    :param key: 字典的key, 默认选用DataFrame的index
    :return: dict
    """
    if isinstance(df, pd.DataFrame):
        rst = dict()
        columns = df.columns.tolist()
        if key and key in set(columns):
            columns.remove(key)
            for index, row in df.iterrows():
                rst[row[key]] = row[columns].tolist()
            return rst
        elif key and key not in set(columns):
            print("key Only choose from DataFrame Columns, you input key {} not exists in columns".format(key))
            return None
        else:  # 默认使用index作为字典的索引
            for index, row in df.iterrows():
                rst[str(index)] = row[columns].tolist()
            return rst
    else:
        print("Only input DataFrame, you input {}".format(type(df)))
        return None


def str2bool(s: str) -> bool:
    """
    :param s:
    :return:
    """
    if not isinstance(s, str):
        s = str(s)
    if s in ['true', 'True', '1', 'y', 'yes', 'yeah', 'yup', 'certainly', 'uh-huh']:
        return True
    else:
        return False


def dict2name_bus(dic):
    """
    此函数完全业务相关,作用是后端与前端字段名称一一对应
    :param dic: 原始字典 {'@id': 1, 'td': '2019072600', 'code': '600610', 'codeType': 'XSHG.ESA.M', 'status': 'HALT',
                        'riseFallRate': 0.0, 'close': 3.26, 'limitHigh': 3.42, 'limitLow': 3.1}
    :return: 转换后的字典 {'time': '2019072600', 'shrCode': '600610', 'type': '14601|14602', 'ratio': 0.0, 'close': 3.26,
                        'typ': '14901|14906'}
    """
    if isinstance(dic, dict):
        keys = dic.keys()
        if '@id' in keys:
            del dic['@id']
        if 'td' in keys:  # 改变keys名称
            dic['time'] = dic['td'] if len(dic['td']) <= 14 else dic['td'][:14]
            # 转换时间格式yyyymmddhhmmSS -> yyyy-mm-dd HH:MM:SS
            tmp_time = dic['time']
            dic['td'] = dic['time']
            try:
                # 修改时间格式
                dic['time'] = '{}-{}-{} {}:{}:{}'.format(tmp_time[:4], tmp_time[4:6], tmp_time[6:8],
                                                         tmp_time[8:10], tmp_time[10:12], tmp_time[12:])
            except Exception as e:
                pass
            # del dic['td']
        if 'code' in keys:
            dic['shrCode'] = dic['code']
            del dic['code']
        if 'riseFallRate' in keys:
            dic['ratio'] = dic['riseFallRate']
            del dic['riseFallRate']
        if 'codeType' in keys:
            dic['typ'] = CONSTANT.MARKET_CODE[dic['codeType']]
            del dic['codeType']
        # 如果存在status, limitHigh, limitLow key 删除
        for e in ['status', 'limitHigh', 'limitLow']:
            dic.pop(e, None)
        return dic
    else:
        return None


def fiveMinuteBefore2Int(t1, fmt):
    """
    convert before 5 minute to Integer
    :param t1: Integer
    :param fmt: yyyymmddHHMMSSfff
    :return:
    """
    tmp_td = (datetime.datetime.strptime(str(t1), fmt) - datetime.timedelta(minutes=5)).strftime(fmt)
    if len(tmp_td) > 17:  # yyyymmddhhmmssSSS
        tmp_td = tmp_td[:17]
    else:
        tmp_td = tmp_td+'0'*(17-len(tmp_td))
    return int(tmp_td)


def timeComparison(t1, t2, fmt):
    """
    check t1 > t2 = 1, t1 < t2 = -1, t1 == t2 = 0
    :param t1:
    :param t2:
    :param fmt:
    :return:
    """
    pass


def fiveMinuteCal(prev_list: list, td: int, close: float) -> (list, float, int):
    """
    计算五分钟涨跌幅, 此函数业务相关，未来应该独立出去
    :param prev_list:
    :param td:
    :param close:
    :return:
    """
    if len(str(td)) < 17:  # yyyymmddHHMMSSsss
        td = int(str(td) + '0' * (17 - len(str(td))))
    if not prev_list:
        prev_list = [(td, close)]
        return prev_list, 0, td
    else:
        prev_list.append((td, float(close)))
        prev_list = sorted(prev_list, key=itemgetter(0))
        try:
            ratio = (prev_list[-1][1] - prev_list[0][1])*100/prev_list[0][1]
        except ZeroDivisionError as e:
            ratio = 0
        except Exception as e:
            print(e)
            pass
        # before_5min
        before_five_minute = fiveMinuteBefore2Int(str(prev_list[-1][0]), CONSTANT.STOCK_TIMESTAMP_FORMAT)
        for i in range(len(prev_list)):
            if prev_list[i][0] < before_five_minute:
                continue
            prev_list = prev_list[i:]
            break
        return prev_list, ratio, prev_list[-1][0]


def kill_pid_file(name):
    """
    write a pid file from python
    :param name: 进程文件名
    :return:
    """
    if os.path.exists(name):
        try:
            with open(name, 'r') as fp:
                line = fp.readline()
                cnt = 1
                while line:
                    print("Line {}: {}".format(cnt, line.strip()))
                    # kill old pid
                    kill_pid(int(line))
                    line = fp.readline()
                    cnt += 1
            # 清空文件内容
            deleteContent(name)
        except Exception as e:
            print(e)


def write_pid_file(fname, pid, name):
    """
    将所有进程pid写入文件同一个文件
    :param fname:
    :param pid:
    :param name:
    :return:
    """
    with open(fname, 'a+') as fp:
        if isinstance(pid, int):
            pid = str(pid)
        fp.write(pid+'\n')
        print("Save {} PID: {} in file:{}".format(name, pid, fname))


def kill_pid(pid):
    """
    清除PID
    :param pid:
    :return:
    """

    try:
        os.kill(pid, signal.SIGTERM)  # or signal.SIGKILL
        print("kill pid: {}, success!".format(pid))
    except Exception as e:
        print(e)


def deleteContent(fname):
    """
    delete the content of file in python
    :param fname: file name
    :return:
    """
    with open(fname, "w"):
        pass


def dateFormatConvert(dt, sformat, dformat):
    """
    时间格式转换
    :param dt: str 时间字符串
    :param sformat: source format 源数据格式
    :param dformat: destination format 目标数据格式
    :return: str
    """
    if isinstance(dt, str):
        dt = str(dt)
    try:
        sourceDate = datetime.datetime.strptime(dt, sformat)
        return sourceDate.strftime(dformat)
    except Exception as e:
        raise Exception(e)
