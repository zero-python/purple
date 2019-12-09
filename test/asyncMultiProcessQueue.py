#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
How share an asynchronous queue with a subprocess?
    > non-blocking
    > asyncio.Queue(maxsize=0,*,loop=None) A first in, first out (FIFO) queue
    if maxsize is less than or equal to zero, the queue size is infinite. If it
    is an integer greater than 0, then await put() blocks when the queue reaches
    maxsize until an item is removed by get().
    > multiprocessing.Queue([maxsize]): Returns a process shared queue implemented
    using a pipe and a fre locks/semaphores. When a process first puts an item on
    the queue a feeder thread is started which transfers objects from a buffer into
    the pipe.
        put_nowait():
        get_nowait():
"""
import asyncio
from multiprocessing import Manager, cpu_count
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


def AsyncProcessQueue(maxsize=0):
    """
    :param maxsize:
    :return:
    """
    # multiprocessing.Manager(): Returns a started SyncManager object which can be used for
    # sharing objects between processes.
    m = Manager()
    # multiprocessing.managers.SyncManager.Queue is used to share between processes.
    q = m.Queue(maxsize=maxsize)
    return _ProcQueue(q)


class _ProcQueue(object):
    def __init__(self, q):
        self._queue = q
        self._real_executor = None
        self._cancelled_join = False

    @property
    def _executor(self):
        if not self._real_executor:
            # ProcessExecutor
            self._real_executor = ProcessPoolExecutor(max_workers=cpu_count())
            # ThreadExecutor
            # self._real_executor = ThreadPoolExecutor(max_workers=cpu_count())
        return self._real_executor

    def __getstate__(self):
        """
        If the class defines the method __getstate__(), it is called and the returned object is pickle
        as the contents for the instance, instead of the contents of the instance's directory.
        :return:
        """
        self_dict = self.__dict__
        self_dict['_real_executor'] = None
        return self_dict

    def __getattr__(self, name):
        """
        This method should either return the (computed) attribute value or raise an AttributeError exception.`
        :param name:
        :return:
        """
        if name in ['qsize', 'empty', 'full', 'put', 'put_nowait',
                    'get', 'get_nowait', 'close']:
            return getattr(self._queue, name)
        else:
            raise AttributeError("'%s' object has no attribute '%s" %
                                 (self.__class__.__name__, name))

    @asyncio.coroutine
    def coro_put(self, item):
        loop = asyncio.get_event_loop()
        return (yield from loop.run_in_executor(self._executor, self.put, item))

    @asyncio.coroutine
    def coro_get(self):
        loop = asyncio.get_event_loop()
        return (yield from loop.run_in_executor(self._executor, self.get))

    def cancel_join_thread(self):
        self._cancelled_join = True
        self._queue.cancel_join_thread()

    def join_thread(self):
        self._queue.join_thread()
        if self._real_executor and not self._cancelled_join:
            self._real_executor.shutdown()


@asyncio.coroutine
def _do_coro_proc_work(q, stuff, stuff2):
    ok = stuff + stuff2
    print("Passing %s to parent" % ok)
    yield from q.coro_put(ok)  # Non-blocking
    item = q.get()  # Can be used with the normal blocking API, too
    print("got %s back from parent" % item)


def do_coro_proc_work(q, stuff, stuff2):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_do_coro_proc_work(q, stuff, stuff2))


@asyncio.coroutine
def do_work(q):
    loop.run_in_executor(ProcessPoolExecutor(max_workers=1),
                         do_coro_proc_work, q, 1, 2)
    item = yield from q.coro_get()
    print("Got %s from worker" % item)
    item += 25
    q.put(item)


if __name__ == '__main__':
    q = AsyncProcessQueue()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(do_work(q))