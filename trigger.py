# -*- coding: utf-8 -*-
"""
purple.trigger
~~~~~~~~~~~~~~

This module provides a
:copyright: (c) 2019 by zero
:license:
"""
import time
from datetime import datetime
import sys
import pandas as pd
from quotations.plate.purple.compat import is_redispy2, is_redispy3
from quotations.manager.redisManager import RedisManager
from quotations.manager.mysqlManager import MysqlManager
from quotations.plate.purple.utils import batcher
from quotations.plate.purple.utils import create_table
import quotations.plate.purple.utils as purple_utils
from quotations.plate.purple import msgConstant as CONSTANT
from quotations.models.fundamental import Ydchange  # 异动表


PROJECT_CODE = CONSTANT.PURPLE_INFO


class PurpleMixin(object):

    def __init__(self):
        pass

    @staticmethod
    def _decorator(func):
        def magic(self):
            print("start magic")
            func(self)
            print("end magic")
        return magic

    @staticmethod
    def _timeit(func):
        """ Python decorator to measure the execution time of methods

        :param func: function | methods
        """
        def timed(self, *args, **kwargs):
            ts = time.time()
            result = func(self, *args, **kwargs)
            te = time.time()
            print('[ %s ] %s(%s, %s) %2.2f sec' % (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                   func.__qualname__, args, kwargs, (te - ts)))
            return result
        return timed


class Trigger(PurpleMixin):
    """异动功能类"""

    def __init__(self):
        super().__init__()
        self._redis = RedisManager('bus_15')
        self._mysql = MysqlManager

    def __enter__(self):
        # in __enter__
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # in __exit__
        self._redis.close()

    @PurpleMixin._timeit
    def sync_code_name(self):
        """同步Wind代码,名称到热存储Redis
        需要每天同步，防止出现新股
        """
        try:
            sql = """SELECT DISTINCT S_INFO_WINDCODE as code, S_INFO_NAME as name
            FROM asharedescription 
            WHERE S_INFO_LISTDATE IS NOT NULL AND S_INFO_DELISTDATE IS NULL;"""
            rst = self._mysql('wind').read_sql(sql, to_DataFrame=True)
            pipe = self._redis.pipeline()
            for index in range(len(rst)):
                # 只存储股票代码
                pipe.hmset("{}:{}".format(PROJECT_CODE, rst.loc[index]['code'][:6]),
                           mapping={'name': rst.loc[index]['name'],
                                    'code': rst.loc[index]['code'],
                                    'codeNoSuffix': rst.loc[index]['code'][:6],
                                    'breakthroughHighPriceFlag': 0,     # 突破30日最高价标志位
                                    'breakthroughLowPriceFlag': 0,      # 突破30日最低价标志位
                                    'breakthroughHighRatioFlag': 0,     # 突破当日涨幅标志位
                                    'breakthroughLowRatioFlag': 0,      # 突破当日跌幅标志位
                                    'uplimitFlag': 0,                   # 涨停标志位
                                    'downlimitFlag': 0,                 # 跌停标志位
                                    'fiveMinuteHighPriceFlag': 0,       # 五分钟涨幅标志位 -- 存储的是时间戳
                                    'fiveMinuteLowPriceFlag': 0,        # 五分钟跌幅标志位 -- 存储的是时间戳
                                    'halfFlag': 0,                      # 盘中临时停盘标志位
                                    'suspFlag': 0,                      # 盘中停盘标志位
                                    })
            pipe.execute()
            print("同步存储Count:{} Wind代码，名称finished, 请查询Redis， db:15, key: {}".format(len(rst), PROJECT_CODE))
        except Exception as e:
            print(e)
        finally:
            print("This is Try Except Finally")

    @PurpleMixin._timeit
    def sync_code_name2node(self):
        """同步Wind代码,名称到热存储Redis
        需要每天同步,防止出现新股,需求node需要
        """
        try:
            # AshareDescription - 中国A股基本资料
            sql = """SELECT S_INFO_WINDCODE as code, S_INFO_NAME as name
            FROM asharedescription 
            WHERE S_INFO_LISTDATE IS NOT NULL AND S_INFO_DELISTDATE IS NULL 
            AND S_INFO_LISTDATE <= '{}';""".format(datetime.today().strftime('%Y%m%d'))
            rst = self._mysql('wind').read_sql(sql, to_DataFrame=True)
            old_keys = self._redis.hkeys(CONSTANT.PURPLE_NODE_ALLSTOCKINFO)
            diff_keys = set(old_keys) - set(rst['code'].tolist())
            if diff_keys:
                # Delete keys from hash name
                self._redis.hdel(CONSTANT.PURPLE_NODE_ALLSTOCKINFO, *list(diff_keys))
            pipe = self._redis.pipeline()
            for index in range(len(rst)):
                pipe.hmset(CONSTANT.PURPLE_NODE_ALLSTOCKINFO,
                           mapping={rst.loc[index]['code']: rst.loc[index]['name']})
            pipe.execute()
            print("同步存储Count:{} Wind代码，名称finished, 请查询Redis， db:15, key: {}".format(len(rst), PROJECT_CODE))
        except Exception as e:
            print(e)
        finally:
            print("This is Try Except Finally")

    @PurpleMixin._timeit
    def sync_high_low_price(self, if_refresh=True, days_num=30):
        """
        同步 最高价，最低价，交易日
        :param if_refresh: 是否是全量刷新或者增量刷新
        :param days_num: 存储历史的天数
        :return:
        """
        code_list = list()
        try:
            # in batches of 500 keys
            # scan_iter(match=None, count=None)
            # Make an iterator using the SCAN command so that the client doesn’t need to remember the cursor position.
            for keybatch in batcher(self._redis.scan_iter(match='{}:*'.format(PROJECT_CODE)), 500):
                code_list.extend(list(map(lambda x: x.split(':')[2], filter(lambda key: isinstance(key, str), keybatch))))
            if if_refresh:  # 全量第一次查询
                # 分批查询SQL
                for code in set(code_list):
                    print(code)
                    # -- 存储Redis
                    pipe = self._redis.pipeline()
                    sql = """SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_HIGH, S_DQ_LOW 
                    FROM ashareeodprices 
                    WHERE S_INFO_WINDCODE IN ('{0}.SH', '{0}.SZ') ORDER BY TRADE_DT DESC LIMIT {1}""".format(code, days_num)
                    rst = self._mysql('wind').read_sql(sql, to_DataFrame=True)
                    if not rst.empty:
                        for index in range(len(rst)):
                            score = int(rst.iloc[index]['TRADE_DT'])
                            high = float(rst.iloc[index]['S_DQ_HIGH'])
                            low = float(rst.iloc[index]['S_DQ_LOW'])
                            dict_value = {'code': code,
                                          'dt': rst.iloc[index]['TRADE_DT'],
                                          'high': high,
                                          'low': low}
                            # json.dumps() converts a dictionary to str object
                            store_value = purple_utils.dict2json(dict_value)
                            if is_redispy3:
                                # 插入redis
                                pipe.zadd(name="purple:30Days:{}".format(code), mapping={store_value: score})
                            if is_redispy2:
                                # 插入redis
                                pipe.zadd("purple:30Days:{}".format(code[:6]), score, store_value)
                        pipe.hset("purple:info:{}".format(code[:6]), 'high', rst['S_DQ_HIGH'].max())
                        pipe.hset("purple:info:{}".format(code[:6]), 'low', rst['S_DQ_LOW'].min())
                        pipe.execute()
                    else:
                        print("{} wind databse no data".format(code))
            else:  # 增量每天收盘之后刷新
                sql = "SELECT S_INFO_WINDCODE, TRADE_DT, S_DQ_HIGH, S_DQ_LOW " \
                      "FROM ashareeodprices " \
                      "WHERE TRADE_DT = (SELECT TRADE_DT FROM ashareeodprices ORDER BY TRADE_DT DESC LIMIT 1)"
                rst = self._mysql('wind').read_sql(sql, to_DataFrame=True)
                rst.rename(columns={"S_INFO_WINDCODE": "code",
                                    "TRADE_DT": "dt",
                                    "S_DQ_HIGH": "high",
                                    "S_DQ_LOW": "low"}, inplace=True)
                # 从Redis获取所有最新code数据
                for index in range(len(rst)):
                    pipe = self._redis.pipeline()
                    # 从redis获取json转化为dataframe
                    days_rst = self._redis.zrange("purple:30Days:{}".format(rst.iloc[index]['code'][:6]), 0, -1, True)
                    days_df = purple_utils.json2df(days_rst)
                    after_days_df = days_df.append(rst.iloc[index]).sort_values(
                        by=['dt'], ascending=False).drop_duplicates(subset=['dt'], keep='last')
                    if len(after_days_df) > len(days_df):
                        # print("新增加数据, {}-{}".format(rst.iloc[index]['code'], rst.iloc[index]['dt']))
                        # 增加新数据
                        dict_value = {'code': rst.iloc[index]['code'][:6],
                                      'dt': rst.iloc[index]['dt'],
                                      'high': float(rst.iloc[index]['high']),
                                      'low': float(rst.iloc[index]['low'])}
                        # json.dumps() converts a dictionary to str object
                        # store_value = json.dumps(dict_value)
                        store_value = purple_utils.dict2json(dict_value)
                        if is_redispy3:
                            # 插入redis
                            pipe.zadd(name="purple:30Days:{}".format(rst.iloc[index]['code'][:6]),
                                      mapping={store_value: int(rst.iloc[index]['dt'])})
                        if is_redispy2:
                            # 插入redis
                            pipe.zadd("purple:30Days:{}".format(rst.iloc[index]['code'][:6]),
                                      int(rst.iloc[index]['dt']), store_value)
                        # 修改最高价，最低价
                        pipe.hset("purple:info:{}".format(rst.iloc[index]['code'][:6]),
                                  'high', after_days_df[:days_num]['high'].max())
                        pipe.hset("purple:info:{}".format(rst.iloc[index]['code'][:6]),
                                  'low', after_days_df[:days_num]['low'].min())
                    if len(after_days_df) > days_num:
                        print("统计近30天多余数据, {}-{}".format(rst.iloc[index]['code'][:6], rst.iloc[index]['dt']))
                        # 删除多余的
                        pipe.zremrangebyscore("purple:30Days:{}".format(after_days_df.iloc[days_num]['code'][:6]),
                                              0, int(after_days_df.iloc[days_num]['dt']))
                    pipe.execute()
        except Exception as err:
            print(err)
        finally:
            print("This is Try Except Finally")

    @PurpleMixin._timeit
    def get_high_low_price(self):
        """
        获取前30天的最高价、最低价
        :return: DataFrame
        """
        try:
            pipe = self._redis.pipeline()
            key_list = list()
            rst = list()
            for keybatch in batcher(self._redis.scan_iter(match='{}:*'.format(PROJECT_CODE)), 500):
                key_list.extend(list(map(lambda x: x, filter(lambda key: isinstance(key, str), keybatch))))
            # 分批获取Redis
            for index, key in enumerate(key_list):
                pipe.hmget(name=key, keys=('codeNoSuffix', 'name', 'high', 'low',
                                           'breakthroughHighPriceFlag',     # 突破30日交易日最高价标志位
                                           'breakthroughLowPriceFlag',      # 突破30日交易日最低价标志位
                                           'breakthroughHighRatioFlag',     # 突破当日涨幅标志位
                                           'breakthroughLowRatioFlag',      # 突破当日跌幅标志位
                                           'uplimitFlag',                   # 涨停标志位
                                           'downlimitFlag',                 # 跌停标志位
                                           'fiveMinuteHighPriceFlag',       # 五分钟涨幅标志位 -- 存储的是时间戳
                                           'fiveMinuteLowPriceFlag',        # 五分钟跌幅标志位 -- 存储的是时间戳
                                           'halfFlag',                      # 盘中临时停盘标志位
                                           'suspFlag'))                     # 盘中停盘标志位

                if index % 1000 == 0 or index == len(key_list)-1:
                    tmp_rst = pipe.execute()
                    rst.extend(tmp_rst)
            return pd.DataFrame(rst, columns=['code', 'name', 'high', 'low',
                                              'breakthroughHighPriceFlag',
                                              'breakthroughLowPriceFlag',
                                              'breakthroughHighRatioFlag',
                                              'breakthroughLowRatioFlag',
                                              'uplimitFlag',
                                              'downlimitFlag',
                                              'fiveMinuteHighPriceFlag',
                                              'fiveMinuteLowPriceFlag',
                                              'halfFlag',
                                              'suspFlag'])
        except Exception as e:
            raise Exception('[] Valar Morghulis []: ', e)
        finally:
            pass

    @PurpleMixin._timeit
    def migrate_redis2mysql(self):
        """
        迁移redis异动信息到mysql, 黄伟产品经理 killed 此功能
        :return:
        """
        pass

    @PurpleMixin._timeit
    def scavenger_redis(self, num=1):
        """
        清理redis历史缓存数据, 非交易日不运行此程序
        :return:
        """
        if isinstance(num, int) and num < 0:
            num = 1
        # 当前日期 20190730
        today = datetime.today().strftime('%Y%m%d')
        # 获取上一交易日日期
        sql = "SELECT TRADE_DAYS FROM asharecalendar WHERE S_INFO_EXCHMARKET = 'SSE' AND TRADE_DAYS < '{}' " \
              "ORDER BY TRADE_DAYS DESC LIMIT {}".format(today, num)
        rst = self._mysql('wind').read_sql(sql, to_DataFrame=True)
        split_date = int(rst["TRADE_DAYS"].tolist()[-1] + "090000")
        print("获取上一交易日期: {}".format(rst["TRADE_DAYS"].tolist()[-1]))
        try:
            pipe = self._redis.pipeline()
            key_list = list()
            for keybatch in batcher(self._redis.scan_iter(match='{}:*'.format(CONSTANT.PURPLE_SAVE)), 500):
                key_list.extend(list(map(lambda x: x, filter(lambda key: isinstance(key, str), keybatch))))

            # 分批获取Redis
            for index, key in enumerate(key_list):
                # 尝试删除order set 中score小于split_date,保留近两天的异动信息
                # Remove all elements in the sorted set name with scores between min and max.
                # Returns the number of elements removed.
                pipe.zremrangebyscore(key, 0, split_date)
                if index % 100 == 0 or index == len(key_list) - 1:
                    pipe.execute()
            pipe.execute()
            print("Any program is only as good as it is useful... See ya next trade day.")
        except Exception as e:
            raise Exception('[] Valar Morghulis []: ', e)
        finally:
            pass

    @PurpleMixin._timeit
    def save2mysqldb(self, df):
        """
        存储异动信息到mysql数据库
        :param df: DataFrame 历史异动信息
        :return:
        """
        try:
            # 核对 ydchange 表是否存在
            create_table(model=Ydchange, engine=self._mysql('hq').engine)
        except Exception as err:
            print(err)
        finally:
            pass

    @PurpleMixin._timeit
    def exceptionQueryRedis(self):
        """
        清理redis异常历史异动缓存数据清理
        :return:
        """
        # 当前日期 20190730
        today = datetime.today().strftime('%Y%m%d')
        # 获取上一交易日日期
        sql = "SELECT TRADE_DAYS FROM asharecalendar WHERE S_INFO_EXCHMARKET = 'SSE' AND TRADE_DAYS <= '{}' " \
              "ORDER BY TRADE_DAYS DESC LIMIT 1".format(today)
        rst = self._mysql('wind').read_sql(sql, to_DataFrame=True)
        split_date = int(rst["TRADE_DAYS"].tolist()[-1] + "153000")
        print("获取最近交易日期: {}".format(rst["TRADE_DAYS"].tolist()[-1]))
        try:
            pipe = self._redis.pipeline()
            key_list = list()
            for keybatch in batcher(self._redis.scan_iter(match='{}:*'.format(CONSTANT.PURPLE_SAVE)), 500):
                key_list.extend(list(map(lambda x: x, filter(lambda key: isinstance(key, str), keybatch))))
            rst_list = []
            # 分批获取Redis
            for index, key in enumerate(key_list):
                # 尝试删除order set 中score小于split_date,保留近两天的异动信息
                # Remove all elements in the sorted set name with scores between min and max.
                # Returns the number of elements removed.
                pipe.zremrangebyscore(key, split_date, sys.maxsize)
                if index % 100 == 0 or index == len(key_list) - 1:
                    rst_list.extend(pipe.execute())
            rst_list.extend(pipe.execute())
            print("Any program is only as good as it is useful... See ya next trade day.\n{}".format(rst_list))
        except Exception as e:
            raise Exception('[] Valar Morghulis []: ', e)
        finally:
            pass


if __name__ == '__main__':
    trigger = Trigger()
    # trigger.init_balabala()
    # 每天收盘之后指定任务-1: 同步股票代码+名称 、 同步前30天的最高价最低价
    # trigger.sync_code_name()
    # trigger.sync_high_low_price(if_refresh=True)
    # trigger.sync_high_low_price(if_refresh=False)

    # 每天开盘前指定任务-2: 获取股票代码、名称、30天最高价、30天最低价
    # trigger.get_high_low_price()

    # 交易日开盘清除上一交易日之前的异动信息
    # trigger.scavenger_redis()

    # 存储异动信息到MySQL数据库
    # trigger.save2mysqldb()

    # 删除异常数据
    # trigger.exceptionQueryRedis()

    # 测试node -- Shaggy
    # trigger.sync_code_name2node()