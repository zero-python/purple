#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
For CPU-bound tasks under CPython, you need multiple processes rather than
multiple threads to have any change of getting a speedup.
"""
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp


def factorize_native(num):
    pass


# example 1
def pool_factorizer_map(nums, nprocs):
    # Let the executor divide the work among processes by using 'map'
    with ProcessPoolExecutor(max_workers=nprocs) as executor:
        return {num: factors for num, factors in zip(nums, executor.map(factorize_native, nums))}


# example 2
def mp_factorizer_map(nums, nprocs):
    with mp.Pool(nprocs) as pool:
        return {num: factors for num, factors in zip(nums, pool.map(factorize_native, nums))}