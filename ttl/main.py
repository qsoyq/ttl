from __future__ import annotations

import asyncio
import asyncio as aio
import threading
import time
import uuid

from dataclasses import dataclass, field
from functools import partial, wraps
from heapq import heappop, heappush
from inspect import iscoroutinefunction
from typing import Any, Callable, List, Optional, Tuple, Union

TTL = int | float  # 过期时间 单位(秒)
memo: dict[Tuple[int, int], CacheResult] = {}
pq: List[PrioritizedItem] = []
_LOCK = threading.Lock()
_ALOCK = asyncio.locks.Lock()


@dataclass(order=True)
class PrioritizedItem:
    priority: Union[int, float]
    item: Tuple[int, int] = field(compare=False)


@dataclass
class CacheResult:

    value: Any
    expire_time: Union[int, float]


async def _get_cache(key: Tuple[int, int]):
    return memo.get(key)


async def _set_cache(key: Tuple[int, int], val: CacheResult):
    global memo
    memo[key] = val
    heappush(pq, PrioritizedItem(priority=val.expire_time, item=key))


def _make_key(*args, **kwargs) -> int:
    key = args
    if kwargs:
        key += (object, )
        for item in kwargs.items():
            key += item
    return hash(key)


def _clear_expired():
    """移除过期条目

    写入缓存结果时, 以过期时间作为优先级, 维护优先级队列.
    按优先级移除缓存结果中的所有过期条目.
    """
    global memo, pq
    now = time.time()
    while pq and pq[0].priority < now:
        item = heappop(pq)
        key = item.item
        cache = memo.get(key)
        if cache and cache.expire_time < now:
            memo.pop(key)


def ttl_cache(fn: Optional[Callable] = None, *, timeout: Union[int, float] = 2) -> Callable:

    if fn is None:
        return partial(ttl_cache, timeout=timeout)

    if iscoroutinefunction(fn):

        @wraps(fn)
        async def aiowrapper(*args, **kwargs):
            async with _ALOCK:
                _clear_expired()
                now = time.time()
                key = (id(fn), _make_key(*args, **kwargs))
                result: Optional[CacheResult] = await _get_cache(key)
                if result is not None and result.expire_time > now:
                    return result.value

                value = await fn(*args, **kwargs)
                expire_time = time.time() + value[1]
                result = CacheResult(value[0], expire_time)
                await _set_cache(key, result)
                return result.value

        return aiowrapper
    else:

        @wraps(fn)
        def wrapper(*args, **kwargs):
            with _LOCK:
                _clear_expired()
                now = time.time()
                key = (id(fn), _make_key(*args, **kwargs))
                result: Optional[CacheResult] = asyncio.run(_get_cache(key))
                if result is not None and result.expire_time > now:
                    return result.value

                value = fn(*args, **kwargs)
                expire_time = time.time() + value[1]
                result = CacheResult(value[0], expire_time)
                asyncio.run(_set_cache(key, result))
                memo[key] = result

                return result.value

        return wrapper


ttl_cache._raw = memo
ttl_cache.clear = memo.clear
ttl_cache.clear_expired = _clear_expired


async def test():
    cost = 0.1
    ttl = 0.1

    @ttl_cache
    async def foo(a: Any) -> tuple[uuid.UUID, TTL]:
        await aio.sleep(cost)
        return uuid.uuid1(), ttl

    task = aio.create_task(foo(0))
    assert await foo(0) == await task
    assert isinstance(await task, uuid.UUID)

    await aio.sleep(ttl)
    assert await foo(0) != await task


if __name__ == '__main__':
    aio.run(test())
