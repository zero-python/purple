#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
async logger command
:copyright: (c) 2019 by zero
aiologger aims to be the standard Asynchronous non blocking logging for python and asyncio

Too Long, Didn't Read; aiologger is only fully async when logging to stdout/stderr.
If you log into files on disk you are not being fully async and will be using Threads.

Despite everything (in Linux) being a file descriptor, a Network file descriptor and
the stdout/stderr FDs are treated differently from files on disk FDs. This happens because
there's no stable/usable async I/O interface published by the OS to be used by Python(or
any other language). That's why logging to files is NOT truly async. aiologger implementation
of file logging uses aiofiles, which uses a Thread Pool to write the data. Keep this in mind
when using aiologger for file logging.
"""