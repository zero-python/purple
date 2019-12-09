设计文档
=======

业务需求[2019-07-25] - PV-3.9.0[异动计算]
---------------------------------------

* [30日新高低]
> 当前Tick价格突破前30交易日最高价和最低价 [ 每日 ⏰ 一次 ]
    
    - 保存数据内容
        * [时间]: Tick YYYY-mm-dd HH:MM:SS 
        * [股票代码]: code 
        * [股票简称]: 中文名称 
        * [提醒文案]: 14601: 30日新高, 14602: 30日新低
        * [最新价格]: current_price , 今日涨跌(current_price - open_price)/open_price, 今日涨幅(current_price - open_price)
    - 每日收盘更新30日最高价,最低价,交易日热数据到 Redis
    

* [快速涨跌]
> 股票在过去五分钟内涨跌幅达到±1% [ 每日 ⏰ 多次 ]
    
    - 保存数据内容
        * [时间]: Tick YYYY-mm-dd HH:MM:SS 
        * [股票代码]: code 
        * [股票简称]: 中文名称 
        * [提醒文案]: 14603: 快速涨, 14604: 快速跌
        * [涨跌幅]: (current_price - last_5min_price)/last_5min_price <> ±1%
        * [最新价格]: current_price 


* [大幅涨跌]
> 股票当日涨跌幅达到±7%时 [ 每日 ⏰ 一次 ]
    
    - 保存数据内容
        * [时间]: Tick YYYY-mm-dd HH:MM:SS 
        * [股票代码]: code 
        * [股票简称]: 中文名称 
        * [提醒文案]: 14605: 大幅涨, 14606: 大幅跌
        * [涨跌幅]: (current_price - open_price)/open_price <> ±7%
        * [最新价格]: current_price 

* [涨跌停]
> 股票当日涨跌停 [ 每日 ⏰ 多次 ]
    
    - 保存数据内容
        * [时间]: Tick YYYY-mm-dd HH:MM:SS  
        * [股票代码]: code 
        * [股票简称]: 中文名称 
        * [提醒文案]: 14607: 涨停, 14608: 跌停
        * [涨跌幅]: (current_price - open_price)/open_price
        * [最新价格]: current_price 

* [涨跌停开盘]
> 股票在今日涨跌停后又重新交易 [ 每日 ⏰ 多次 ]
    
    - 保存数据内容
        * [时间]: Tick YYYY-mm-dd HH:MM:SS
        * [股票代码]: code 
        * [股票简称]: 中文名称
        * [提醒文案]: 14609: 涨停开板, 1410: 跌停开板
        * [最新价格]: current_price, (current_price - open_price)/open_price

        
业务需求[2019-09-02] - PV-3.9.1[科创板]
-------------------------------------
* [行情-科创板卡片]
> 日涨幅最高个股名称、价格、涨跌幅 [定时三秒刷新]
    - 创建科创板:KSH_riseAndFallRate (行情刷新 - 涨跌幅)
    - 创建科创板:KSH_turnOver (行情刷新 - 换手率)
* [科创板-新股动态]
    - 标记每个日期下的科创板新股信息(今日申购/今日上市/中签公示) -- NODE 
    - 统计每日科创板新股信息数量 
* [个股标签]
    -  增加科创板标签 ksh 1, 0 
    -  盘中临时停盘标签 status_half 1, 0 
    -  盘中停盘标签 status_susp 1, 0 
    -  停盘：停盘期间()
    ... 
* [分时数据存储]
    - 科创板特殊代码分时数据处理
    
* [盘中临时停盘 TRADE - HALT]
> 股票在今日交易过程中出现临时停盘 [ 每日 ⏰ 多次 ]
    
    - 保存数据内容
        * [时间]: Tick YYYY-mm-dd HH:MM:SS
        * [股票代码]: code 
        * [股票简称]: 中文名称
        * [提醒文案]: 14611: 盘中临牌, 14612: 恢复交易
        * [最新价格]: 调用其他接口

    
* [盘中停盘 TRADE - SUSP]
> 股票在今日交易过程中出现停盘 [ 每日 ⏰ 一次 ]
    
    - 保存数据内容
        * [时间]: Tick YYYY-mm-dd HH:MM:SS
        * [股票代码]: code 
        * [股票简称]: 中文名称
        * [提醒文案]: 14613: 盘中停牌，14614：盘中停牌恢复交易
        * [最新价格]: 调用其他接口

FAQ
---
```
每日行情Tick数据大约为千万行数据    
```

Technology Stack
----------------
```
Protocol Buffer:
    Define message formats in a .proto file.
    Use the prptocol buffer compiler 
        C++: the compiler generates a .h and .cc file from each .proto 
        Java: protoc -I=$SRC_DIR --java_out=$DST_DIR $SRC_DIR/addressbook.proto 
            # import a jar file in Eclipse 
            $ wget https://repo1.maven.org/maven2/com/google/protobuf/protobuf-java/3.7.1/protobuf-java-3.7.1.jar
        Python: protoc -I=$SRC_DIR --python_out=$DST_DIR $SRC_DIR/addressbook.proto
        Go: protoc -I=$SRC_DIR --go_out=$DST_DIR $SRC_DIR/addressbook.proto
    Use the Java protocol buffer API to write and read messages

RabbitMQ:
    Java: 
        RabbitMQ Java Client Library allows Java code to interface with RabbitMQ.
        $ wget http://repo1.maven.org/maven2/com/rabbitmq/amqp-client/5.7.2/amqp-client-5.7.2.jar
        
        
Language: Java, Python

Redis 测试 -- db15, stockChange 

14601: 30日新高, 14602: 30日新低
14603: 快速涨,   14604: 快速跌
14605: 大幅涨,   14606: 大幅跌
14607: 涨停,     14608: 跌停
14609: 涨停开板,  14610: 跌停开板
14611: 临时停牌， 14612: 临时停牌恢复交易
14613: 盘中停牌， 14614：盘中停牌恢复交易

value
currentPrice|涨跌幅率
```

股票交易实时状态
--------------
```
100:    市场启动(初始化之后、集合竞价之前)
101:    盘前处理 
102:    集合竞价
103:    盘中交易(连续撮合)
104:    暂停交易(盘中临时停盘)
105:    盘中停盘
106:    休市
107:    科创板盘后固定价格交易
108:    盘后
109:    结束交易
110:    长期停盘
111:    退市
```

股市英文表达
----------
```
Shanghai Composite Index: 上证综合指数
Shenzhen Component Index: 深圳成份股指数
HongKong's Hang Seng Index: 香港恒生指数

A-share market: A股市场
B-share market: B股市场
bourse: 证券交易所
trading: 交易、买卖
turnover: 成交额
trading volume: 成交额、成交量
weighting: 权重
morning session: 早盘
afternoon session: 午盘
insider trading: 内幕交易
trader: 交易者
speculator: 投机者
investor:  投资者
broker: 证券经纪人
brokerage: 券商
margin finance account: 保证金融资账户
securities regulator: 证券监管机构
```
