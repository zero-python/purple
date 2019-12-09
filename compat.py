# -*- coding:utf-8 -*-
"""
purple.compat
~~~~~~~~~~~~~

This module handles import compatibility issues
:copyright: (c) 2019 by zero
:license:
"""
import redis


# -----
# Redis
# -----

# Syntax sugar.
_redispy = redis.__version__

#: redis-py version 3.x?
is_redispy3 = (_redispy[0] == '3')

#: redis-py version 2.x?
is_redispy2 = (_redispy[0] == '2')