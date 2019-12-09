# -*- coding: utf-8 -*-
"""
purple.api
~~~~~~~~~~~~~

This module implements the purple API.

:copyright: (c) 2019 by zero
:license:
"""
from quotations.plate.purple import trigger


def sync_30_days_high_low(if_refresh):
    """
    同步历史30天新高价格、新低新低
    :param if_refresh: True | False
    :return:
    """
    with trigger.Trigger() as purple:
        # 每天收盘之后指定任务-1: 同步股票代码+名称 、 同步前30天的最高价最低价
        purple.sync_code_name()
        purple.sync_code_name2node()  # for node api get /getMarketInfo/
        purple.sync_high_low_price(if_refresh)


def get_30_days_high_low():
    """
    获取前30交易日最高价，最低价，代码，名称
    :return: DataFrame
    """
    with trigger.Trigger() as purple:
        return purple.get_high_low_price()


def clean_redis(num=1):
    """
    每天开盘前运行
    定时清理Redis中异动历史数据,默认是删除两天前的数据
    :return:
    """
    with trigger.Trigger() as purple:
        purple.scavenger_redis(num)


if __name__ == '__main__':
    # 同步30日新高新低
    sync_30_days_high_low(if_refresh=False)