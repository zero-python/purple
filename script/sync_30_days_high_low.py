#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
每天开盘前执行一次，为下一交易日提供30交易日前最高价和最低价
清除redis中历史异动信息
交易日开盘: 08:00
"""
from quotations.plate.purple import api
from quotations.util.common import timeit, tradeit


@timeit
@tradeit
def run():
    api.sync_30_days_high_low(if_refresh=False)
    api.clean_redis(num=1)


if __name__ == '__main__':
    run()