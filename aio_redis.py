#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
async redis command
:copyright: (c) 2019 by zero

aioredis: The library is intended to provide simple and clear interface to Redis based on asyncio.

"""
import asyncio
import aioredis
from quotations.conf import config


if __name__ == '__main__':
    print(config('redis').get("bus_15"))
