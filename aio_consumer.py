#!/usr/bin/env python3
# -*- coding:utf-8 -*-
import asyncio
# aio_pika it's a wrapper for the pika for asyncio and humans
import aio_pika
from aio_pika.pool import Pool
import aioredis
from aiologger import Logger
import configparser
import multiprocessing as mp
import os
from quotations.conf import config as CONCONFIG  # Redis, MySQL 连接配置
from quotations.plate.purple import msgConstant as CONSTANT
from quotations.plate.purple import api as purpleApi
from quotations.util.common import tradeit
from quotations.plate.purple.utils import (msg2dict,
                                           check_stock_status,
                                           df2dict,
                                           str2bool,
                                           dict2name_bus,
                                           dict2json,
                                           fiveMinuteCal,
                                           fiveMinuteBefore2Int,
                                           kill_pid_file,
                                           write_pid_file)


config = configparser.ConfigParser()
# configparser KeyError when run as Cronjob
# should use the absolute path to the config file in your script
config_file = os.path.join(os.path.dirname(__file__), 'rabbitmq.ini')
config.read(config_file)
# 获取Redis连接信息
REDIS_DICT = CONCONFIG("redis").get("bus_15")
# 可以按照市场类别进行分类获取数据
df_high_low = purpleApi.get_30_days_high_low()
# 新股没有前30天最高和最低价,初始化一个极大值，极小值
df_high_low['high'] = df_high_low['high'].fillna(99999)
df_high_low['low'] = df_high_low['low'].fillna(-99999)
# 初始化5min数据list --
df_high_low['fiveMinuteRaw'] = None
dict_high_low = df2dict(df_high_low, 'code')


async def aio_redis_dao(conn_pool, task_queue, msg):
    """
    异步交互redis
    :param conn_pool: redis connection pool
    :param task_queue:
    :param msg:
    :return:
    """
    if task_queue:
        msg2 = dict2name_bus(msg)  # 后端与前端字段名称一一对应
        for yd_type in task_queue:
            msg2['type'] = yd_type
            if yd_type == "14601":
                # 赋值30日新高标识位
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'breakthroughHighPriceFlag', 1)
                # 存储30日新高异动信息
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type),
                                        int(msg2['td']), dict2json(msg2))
            elif yd_type == "14602":
                # 赋值30日新低标识位
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'breakthroughLowPriceFlag', 1)
                # 存储30日新低异动信息
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type),
                                        int(msg2['td']), dict2json(msg2))
            elif yd_type.startswith("14603", 0, 5):
                # 覆盖type类型
                msg2['type'] = yd_type[:5]
                ratio = float(yd_type.split(":")[1])
                # 快速涨幅达到1
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'fiveMinuteHighPriceFlag', msg2['td'])
                # 快速涨幅达到1
                tmp = msg2['ratio']
                msg2['ratio'] = ratio
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type[:5]),
                                        int(msg2['td']), dict2json(msg2))
                msg2['ratio'] = tmp

            elif yd_type.startswith("14604", 0, 5):
                # 覆盖type类型
                msg2['type'] = yd_type[:5]
                ratio = float(yd_type.split(":")[1])
                # 快速涨幅达到1
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'fiveMinuteLowPriceFlag', msg2['td'])
                # 快速涨幅达到1
                tmp = msg2['ratio']
                msg2['ratio'] = ratio
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type[:5]),
                                        int(msg2['td']), dict2json(msg2))
                msg2['ratio'] = tmp

            elif yd_type == "14605":
                # 开盘大幅涨 超过7%
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'breakthroughHighRatioFlag', 1)
                # 开盘大幅涨 异动信息
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type),
                                        int(msg2['td']), dict2json(msg2))
            elif yd_type == "14606":
                # 开盘大幅跌 超过7%
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'breakthroughLowRatioFlag', 1)
                # 开盘大幅跌 异动信息
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type),
                                        int(msg2['td']), dict2json(msg2))
            elif yd_type == "14607":
                # 涨停
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'uplimitFlag', 1)
                # 涨停信息
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type),
                                        int(msg2['td']), dict2json(msg2))
            elif yd_type == "14608":
                # 跌停
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'downlimitFlag', 1)
                # 跌停信息
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type),
                                        int(msg2['td']), dict2json(msg2))
            elif yd_type == "14609":
                # 涨停开板
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'uplimitFlag', 0)
                # 涨停开板信息
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type),
                                        int(msg2['td']), dict2json(msg2))
            elif yd_type == "14610":
                # 跌停开板
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'downlimitFlag', 0)
                # 跌停开板信息
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type),
                                        int(msg2['td']), dict2json(msg2))
            elif yd_type == "14611":
                # 临时停牌
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'halfFlag', 1)
                # 临时停牌信息
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type),
                                        int(msg2['td']), dict2json(msg2))
            elif yd_type == "14612":
                # 临时停牌-恢复交易
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'halfFlag', 0)
                # 临时停牌信息-恢复交易
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type),
                                        int(msg2['td']), dict2json(msg2))
            elif yd_type == "14613":
                # 盘中暂停交易
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'suspFlag', 1)
                # 盘中暂停交易信息
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type),
                                        int(msg2['td']), dict2json(msg2))
            elif yd_type == "14614":
                # 暂停交易-恢复交易--不可能出现[保留选项]
                await conn_pool.execute('hset',
                                        '{}:{}'.format(CONSTANT.PURPLE_INFO, msg['shrCode']),
                                        'suspFlag', 0)
                # 临时停牌信息-恢复交易
                await conn_pool.execute('zadd', "{}:{}:{}".format(CONSTANT.PURPLE_SAVE, msg['shrCode'], yd_type),
                                        int(msg2['td']), dict2json(msg2))
            else:
                pass
    else:
        pass


async def stock_trigger(q_name, init_dict):
    """
    异动处理主逻辑
    :param q_name: 队列名称
    :param init_dict: 30日新高新低
    :return:
    """
    logger = Logger.with_default_handlers(name=q_name)
    # Get the current event loop. If there is no current event loop set in the current OS thread and set_event_loop()
    # has not yet been called, asyncio will create a new event loop and set it as the current one.
    loop = asyncio.get_event_loop()
    # async redis connection pool
    redis_loop = await aioredis.create_pool('redis://{}:{}'.format(REDIS_DICT['host'], REDIS_DICT['port']),
                                            db=REDIS_DICT['db'],
                                            password=REDIS_DICT['password'],
                                            minsize=1, maxsize=10, loop=loop)

    async def get_connection():
        return await aio_pika.connect_robust(
            "amqp://{}:{}@{}:{}/{}".format(config['rabbitmq']['username'],
                                          config['rabbitmq']['password'],
                                          config['rabbitmq']['host'],
                                          config['rabbitmq']['port'],
                                          config['rabbitmq']['virtual_host']))

    connection_pool = Pool(get_connection, max_size=2, loop=loop)

    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()

    channel_pool = Pool(get_channel, max_size=10, loop=loop)
    queue_name = q_name

    async def consume():
        async with channel_pool.acquire() as channel:   # type: aio_pika.Channel
            # qos : Quality of service 服务质量
            await channel.set_qos(prefetch_count=50)

            queue = await channel.declare_queue(queue_name, durable=True, auto_delete=False)

            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    msg = msg2dict(message.body)
                    # print(msg)  # 全部
                    if check_stock_status(msg['status']):
                        task_queue = []
                        msg['name'] = init_dict[msg['code']][0]  # 增加股票代码名称字段
                        # 同一条消息会被6个计算单元消费
                        # 1. 判断是否超越30日高、低, 因为每日只提醒一次，需要与Redis交互查询是否已经发送
                        if not str2bool(init_dict[msg['code']][3]) and \
                                float(msg['close']) > float(init_dict[msg['code']][1]):
                            init_dict[msg['code']][3] = '1'
                            task_queue.append("14601")
                            print("14601", msg)
                        elif not str2bool(init_dict[msg['code']][4]) and \
                                float(msg['close']) < float(init_dict[msg['code']][2]):
                            init_dict[msg['code']][4] = '1'
                            task_queue.append("14602")
                            print("14602", msg)
                        else:
                            pass

                        # 2. 五分钟内涨跌幅达到±1%, 每日提醒多次
                        # 先计算五分钟内涨跌幅值 -- 使用斐波那契堆，最小堆+最大堆
                        # 将时间和价格组合成一个tuple (td, close)
                        init_dict[msg['code']][-1], fiveRatio, td = fiveMinuteCal(init_dict[msg['code']][-1],
                                                                                        int(msg['td']),
                                                                                        float(msg['close']))
                        # 五分钟涨幅超过1%,因为重复消息五分钟内仅提醒一次, 需要与Redis交互查询是否五分钟内重复触发
                        if fiveRatio >= 1 and fiveMinuteBefore2Int(td, CONSTANT.STOCK_TIMESTAMP_FORMAT) > int(init_dict[msg['code']][9]):
                            init_dict[msg['code']][9] = td
                            task_queue.append("14603:{}".format(fiveRatio))

                        # 五分钟跌幅超过1%,因为重复消息五分钟内仅提醒一次, 需要与Redis交互查询是否五分钟内重复触发
                        if fiveRatio <= -1 and fiveMinuteBefore2Int(td, CONSTANT.STOCK_TIMESTAMP_FORMAT) > int(init_dict[msg['code']][10]):
                            init_dict[msg['code']][10] = td
                            task_queue.append("14604:{}".format(fiveRatio))

                        # 3. 判断当日涨跌幅是否达到±7%，因为每日只提醒一次，需要与Redis交互查询是否已经提醒
                        if not str2bool(init_dict[msg['code']][5]) and \
                                float(msg['riseFallRate']) > CONSTANT.ALARM_QUOTE_CHANGE:
                            init_dict[msg['code']][5] = '1'
                            task_queue.append("14605")
                            print("14605", msg)
                        elif not str2bool(init_dict[msg['code']][6]) and \
                                float(msg['riseFallRate']) < -CONSTANT.ALARM_QUOTE_CHANGE:
                            init_dict[msg['code']][6] = '1'
                            task_queue.append("14606")
                            print("14606", msg)
                        else:
                            pass

                        # 4. 判断当日涨跌停，因为每日提醒多次，需要与Redis交互查询|本地保存一个字典是否已经提醒
                        if not str2bool(init_dict[msg['code']][7]) and float(msg['limitHigh'] != 0) \
                                and float(msg['close']) >= float(msg['limitHigh']):
                            init_dict[msg['code']][7] = '1'
                            task_queue.append("14607")
                            print("14607", msg)
                        elif not str2bool(init_dict[msg['code']][8]) and float(msg['limitLow'] != 0) \
                                and float(msg['close']) <= float(msg['limitLow']):
                            init_dict[msg['code']][8] = '1'
                            task_queue.append("14608")
                            print("14608", msg)
                        else:
                            pass

                        # 5. 首先出现涨跌停标志，然后判断是否出现涨跌停开板
                        if str2bool(init_dict[msg['code']][7]) and float(msg['limitHigh'] != 0) \
                                and float(msg['close']) < float(msg['limitHigh']):
                            init_dict[msg['code']][7] = '0'
                            task_queue.append("14609")
                            print('14609', msg)
                        elif str2bool(init_dict[msg['code']][8]) and float(msg['limitLow'] != 0) \
                                and float(msg['close']) > float(msg['limitLow']):
                            init_dict[msg['code']][8] = '0'
                            task_queue.append("14610")
                            print('14610', msg)
                        else:
                            pass

                        # 6. 判断是否出现盘中临时停盘、或恢复交易
                        if not str2bool(init_dict[msg['code']][11]) and msg['status'] == 'HALF':
                            init_dict[msg['code']][11] = '1'
                            task_queue.append("14611")
                            print("14611", msg)
                        elif str2bool(init_dict[msg['code']][11]) and msg['status'] == 'TRADE':
                            init_dict[msg['code']][11] = '0'
                            task_queue.append("14612")
                            print("14612", msg)
                        else:
                            pass

                        # 7. 判断是否出现盘中停盘,恢复交易
                        if not str2bool(init_dict[msg['code']][12]) and msg['status'] == 'SUSP':
                            init_dict[msg['code']][12] = '1'
                            task_queue.append("14613")
                            print("14613", msg)
                        elif str2bool(init_dict[msg['code']][12]) and msg['status'] == 'TRADE':
                            # Reasonable，This situation does not appear.
                            init_dict[msg['code']][12] = '0'
                            task_queue.append("14614")
                            print("14614", msg)
                        else:
                            pass

                        # interaction redis
                        await aio_redis_dao(redis_loop, task_queue, msg)
                    else:
                        pass
                    # Confirm message -- 显示确认保证消息不会丢失
                    await message.ack()

    task = loop.create_task(consume())
    await task


def aio_executor(queue_name, init_dict):
    """
    独立进程入口
    :param queue_name:
    :param init_dict:
    :return:
    """
    pid_file = __file__[:-3] + '.pid'
    write_pid_file(pid_file, os.getpid(), '{} Process'.format(mp.current_process().name))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(stock_trigger(queue_name, init_dict))
    loop.close()
    return queue_name


@tradeit
def run():
    task_queues = ['XSHE_ESA_GEM', 'XSHE_ESA_M', 'XSHE_ESA_SMSE', 'XSHG_ESA_M', 'XSHG_KSH']
    procs = []
    pid_file = __file__[:-3] + '.pid'
    kill_pid_file(pid_file)
    write_pid_file(pid_file, os.getpid(), 'Main Process')
    for index, task_queue in enumerate(task_queues):
        proc = mp.Process(target=aio_executor, args=(task_queue, dict_high_low,))
        procs.append(proc)
        proc.start()
    for proc in procs:
        proc.join()


if __name__ == '__main__':
    run()