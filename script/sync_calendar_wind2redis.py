#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
sync wind calendar date to redis
定时任务放在00:05
"""
# Launching parallel tasks
import concurrent.futures
from quotations.conf import config
from dateutil import rrule
from datetime import date, datetime
from pandas.tseries import offsets
from quotations.util.common import timeit
from quotations.manager.mysqlManager import MysqlManager
from quotations.manager.redisManager import RedisManager


# Chyi add
engine_wind = MysqlManager('wind').engine
redisManager3 = RedisManager('bus_3')
# Redis Keys
NO_TRADE_DAYS = "NO_TRADE_DAY"
TRADE_DAYS = "TRADE_DAY"
ALL_DAYS = "THIS_YEAR_DAY"


def get_calendar_days():
    with MysqlManager('wind') as session:
        try:
            year_begain = (date.today() - offsets.YearBegin()).strftime('%Y%m%d')
            year_end = (date.today() + offsets.YearEnd()).strftime('%Y%m%d')
            # AShareCalendar -- 中国A股交易日历; SSE -- 上海证券交易所
            sql_fetch_all = """SELECT TRADE_DAYS FROM AShareCalendar 
            WHERE S_INFO_EXCHMARKET = 'SSE' AND TRADE_DAYS BETWEEN {0} AND {1}""".format(year_begain, year_end)
            trade_days = set(session.read_sql(sql_fetch_all, to_DataFrame=True)['TRADE_DAYS'].tolist())
            all_days = set(map(lambda dt: dt.strftime('%Y%m%d'),
                                rrule.rrule(rrule.DAILY,
                                            dtstart=datetime.strptime(year_begain, '%Y%m%d'),
                                            until=datetime.strptime(year_end, '%Y%m%d'))))
            no_trade_days = all_days - trade_days
            return trade_days, no_trade_days
        except Exception as e:
            print(e)
            return None, None
        finally:
            print('pas de bras pas de chocolat. No chocolate without arms.')


def save_calendar_days(days_set, key_name, which_redis):
    """
    更新存储交易日历到redis相应的keys
    :param days_set: set
    :param key_name:
    :param which_redis:
    :return:
    """
    days_set = set(map(lambda dt: '{}-{}-{}'.format(dt[:4], dt[4:6], dt[6:]), days_set))
    try:
        rst = which_redis.hgetall(key_name)
        del_dt = set(rst.keys()) - days_set
        # Delete keys from hash name
        rst_code = which_redis.hdel(key_name, *del_dt) if del_dt else False
        if rst_code:
            print("Delete keys[{}] from hash name:{} ".format(del_dt, key_name))
        # Set key to value within hash name for each corresponding key and value from the mapping dict.
        add_dt = days_set - set(rst.keys())
        rst_code = which_redis.hmset(key_name, mapping=dict.fromkeys(add_dt, 0)) if add_dt else False
        if rst_code:
            print("Set key to value within hash name for each corresponding key and value from the mapping dict.")
        return 0
    except Exception as err:
        print(err)
        return -1
    finally:
        print("You know nothing jon snow. this is try-catch-finally")


@timeit
def run():
    trade_days, no_trade_days = get_calendar_days()
    all_days = trade_days + no_trade_days
    tasks = {
        TRADE_DAYS: trade_days,
        NO_TRADE_DAYS: no_trade_days,
        ALL_DAYS: all_days
    }
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_dict = {executor.submit(save_calendar_days, val, key, redisManager3): key for key, val in tasks.items()}
        for future in concurrent.futures.as_completed(future_dict):
            key = future_dict[future]
            try:
                rst = future.result()
            except Exception as err:
                print('%r generated an exception: %s' % (key, err))
            else:
                print('%r sync success, return code: %d' % (key, rst))


if __name__ == '__main__':
    run()
