# utils/redis_helpers.py
"""
Redis helper for tender-extraction pipeline
-------------------------------------------

We use a Redis list as a task queue:
 - LPUSH queue tasks
 - BRPOP to consume tasks (blocking)
 - Tasks are JSON objects containing:
      { "bid_number": "...", "detail_url": "...", "page": ... }

Install Redis client:
    pip install redis
"""

import json
import redis
from config import REDIS

QUEUE_NAME = "gem_tasks"   # main task queue


def get_redis():
    return redis.Redis(
        host=REDIS.get("host", "127.0.0.1"),
        port=REDIS.get("port", 6379),
        db=REDIS.get("db", 0),
        password=REDIS.get("password", None),
        decode_responses=True
    )


def enqueue_task(task: dict):
    """
    Push a single task into the Redis queue.
    """
    r = get_redis()
    r.lpush(QUEUE_NAME, json.dumps(task))
    return True


def enqueue_batch(tasks: list):
    """
    Push a list of tasks (dicts).
    Efficient bulk enqueue for producer.
    """
    if not tasks:
        return 0

    r = get_redis()
    pipe = r.pipeline()
    for t in tasks:
        pipe.lpush(QUEUE_NAME, json.dumps(t))
    pipe.execute()
    return len(tasks)


def pop_task(block=True, timeout=5):
    """
    Pop a task from the queue.

    block=True  -> BRPOP (waits until task arrives)
    block=False -> RPOP  (returns immediately or None)
    """
    r = get_redis()

    if block:
        res = r.brpop(QUEUE_NAME, timeout=timeout)
        if not res:
            return None  # timeout
        _, payload = res
        return json.loads(payload)

    # non-blocking
    payload = r.rpop(QUEUE_NAME)
    if not payload:
        return None
    return json.loads(payload)


def queue_length():
    """
    Return number of tasks currently in the queue.
    """
    r = get_redis()
    return r.llen(QUEUE_NAME)
