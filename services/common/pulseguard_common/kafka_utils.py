"""Kafka producer/consumer factories shared by every PulseGuard service.

Uses aiokafka so the whole pipeline is asyncio-native: the producer can fire
bursts without blocking, and consumers can process messages concurrently
(configurable worker pool) instead of one-at-a-time.
"""
from __future__ import annotations

import asyncio
import os
from typing import Awaitable, Callable, Optional

import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def serialize(value: dict) -> bytes:
    return orjson.dumps(value)


def deserialize(raw: bytes) -> dict:
    return orjson.loads(raw)


# aiokafka's AIOKafkaProducer validates `acks` against this exact set:
# `if acks not in (0, 1, -1, 'all', _missing): raise ValueError("Invalid
# ACKS parameter")` (aiokafka/producer/producer.py). Note the type
# matters — the *string* "1" is not equal to the *int* 1, so it fails that
# check. Kafka's own wire protocol only understands the integers -1, 0, 1
# anyway ('all' is just aiokafka's client-side alias for -1), so we
# standardize on ints here to avoid that class of bug entirely.
VALID_ACKS = (0, 1, -1)

# aiokafka additionally requires acks to be 'all'/-1 whenever
# enable_idempotence=True (idempotent delivery can only be guaranteed if
# every in-sync replica acknowledges the write) — passing acks=1 with
# idempotence on raises a *different* ValueError
# ("acks=1 not supported if enable_idempotence=True"). We default to
# exactly that valid, stronger-guarantee combination.
DEFAULT_ACKS = -1  # "all" — required alongside enable_idempotence=True


def validate_acks_config(acks, enable_idempotence: bool) -> None:
    """Raises ValueError for any (acks, enable_idempotence) combination
    aiokafka would itself reject — kept as a standalone, network-free
    function so it's directly unit-testable (constructing/starting an
    AIOKafkaProducer requires a live broker; this validation doesn't).
    """
    if acks not in VALID_ACKS:
        raise ValueError(f"acks must be one of {VALID_ACKS} (got {acks!r})")
    if enable_idempotence and acks != -1:
        # Fail fast with a clear message instead of letting aiokafka raise
        # its own less-obvious ValueError deep inside __init__.
        raise ValueError(
            f"enable_idempotence=True requires acks=-1 ('all'), got acks={acks!r}. "
            "Either pass acks=-1 or set enable_idempotence=False."
        )


async def make_producer(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP,
    acks: int = DEFAULT_ACKS,
    linger_ms: int = 5,
    max_batch_size: int = 65536,
    enable_idempotence: bool = True,
) -> AIOKafkaProducer:
    validate_acks_config(acks, enable_idempotence)

    # Deliberately no value_serializer here: callers pass already-serialized
    # bytes via `serialize()`. This lets the simulator inject raw malformed
    # bytes (corrupted-event testing) through the exact same send path.
    #
    # acks=-1 ("all") + enable_idempotence=True is the strongest delivery
    # guarantee aiokafka offers (no duplicate/reordered sends on retry,
    # write acknowledged by every in-sync replica) — and it costs nothing
    # on our hot path specifically because callers use fire-and-forget
    # `producer.send()` (not `send_and_wait()`) and never block on the
    # returned future; the extra broker-side wait for replica acks happens
    # in the background, off the producer loop.
    #
    # compression_type="gzip", not "lz4": aiokafka's lz4 codec needs a
    # separate `lz4` (or `lz4framed`/`lz4f`) pip package that isn't
    # installed anywhere in this project — using it here would raise
    # `RuntimeError: Compression library for lz4 not found` at producer
    # startup, in every service, right after the acks fix. gzip is backed
    # by Python's stdlib (`aiokafka.codec.has_gzip()` is unconditionally
    # True) so it has zero extra dependencies and nothing to fail to build
    # inside the container. Compression ratio/CPU cost isn't the bottleneck
    # at PulseGuard's scale; not having a working producer is worse than a
    # marginally less CPU-efficient codec.
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks=acks,
        linger_ms=linger_ms,
        max_batch_size=max_batch_size,
        compression_type="gzip",
        enable_idempotence=enable_idempotence,
    )
    await producer.start()
    return producer


async def send_with_retry(
    producer: AIOKafkaProducer,
    topic: str,
    value: "bytes | dict",
    key: Optional[bytes] = None,
    max_retries: int = 3,
    base_backoff: float = 0.1,
) -> bool:
    """Send a message, retrying transient Kafka errors with backoff.

    Returns True on success, False if all retries were exhausted (caller is
    expected to route the payload to the dead-letter topic in that case).
    """
    payload = value if isinstance(value, (bytes, bytearray)) else serialize(value)
    attempt = 0
    while attempt <= max_retries:
        try:
            await producer.send_and_wait(topic, value=payload, key=key)
            return True
        except KafkaError:
            attempt += 1
            if attempt > max_retries:
                return False
            await asyncio.sleep(base_backoff * (2 ** (attempt - 1)))
    return False


async def send_to_dead_letter(
    producer: AIOKafkaProducer,
    dead_letter_topic: str,
    original_value: "bytes | dict",
    error_reason: str,
) -> None:
    if isinstance(original_value, (bytes, bytearray)):
        try:
            original_repr = original_value.decode("utf-8", errors="replace")
        except Exception:
            original_repr = repr(original_value)
        envelope = {"original_raw": original_repr, "error_reason": error_reason}
    else:
        envelope = {"original": original_value, "error_reason": error_reason}
    try:
        await producer.send_and_wait(dead_letter_topic, value=serialize(envelope))
    except KafkaError:
        # Last resort: swallow — we never want dead-letter delivery failures
        # to crash the consumer loop.
        pass


def make_consumer(
    topics: list[str],
    group_id: str,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP,
    auto_offset_reset: str = "latest",
    max_poll_records: int = 500,
) -> AIOKafkaConsumer:
    # No value_deserializer: raw bytes are handed to the caller's handler so
    # a JSON-decode failure (e.g. a deliberately corrupted event) can be
    # caught per-message and routed to the dead-letter topic instead of
    # crashing the consumer loop.
    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
        max_poll_records=max_poll_records,
    )


async def run_concurrent_consumer(
    consumer: AIOKafkaConsumer,
    handler: Callable[[bytes], Awaitable[None]],
    concurrency: int = 4,
    commit_every: int = 100,
) -> None:
    """Drive a consumer with a bounded pool of concurrent handler tasks,
    while preserving Kafka's per-partition delivery order.

    Kafka only guarantees message order *within a partition* — never
    across partitions. The original version of this function fired every
    message into an unconstrained pool of concurrent asyncio tasks with no
    ordering relationship between them, so two messages for the same
    symbol/partition (e.g. sequence 41 then 42) could finish processing in
    either order depending on scheduling (e.g. one of them happening to
    also do a slower I/O call, like a dead-letter publish). Every
    consumer here uses per-symbol *stateful* handling — sequence-gap
    detection, duplicate detection, rolling price statistics — so
    processing 42 before 41 corrupts that state and manufactures a false
    SEQUENCE_GAP/anomaly that never actually happened in the stream. That
    inflates alert/anomaly counts independent of anything actually wrong
    with the feed.

    Fix: chain each partition's messages into their own in-order sequence
    (each message's task awaits the previous message's task *for that same
    partition* before running its handler), while different partitions
    still run fully concurrently against each other, bounded by a
    semaphore to `concurrency` messages actually executing at once system
    -wide. This preserves both correctness (per-partition order) and
    throughput (cross-partition parallelism), without requiring any
    change to the `handler` signature.
    """
    sem = asyncio.Semaphore(concurrency)
    # Tail task of each partition's in-order processing chain.
    partition_chain: dict[tuple[str, int], asyncio.Task] = {}
    all_tasks: set[asyncio.Task] = set()
    processed = 0
    # How many messages we allow to be created-but-not-yet-executing before
    # we stop pulling new ones from the consumer and apply backpressure —
    # mirrors the original design's intent (don't let the in-flight/queued
    # work grow unbounded while we wait on slow handlers).
    max_pending = max(concurrency * 4, concurrency)

    async def _run_in_order(prev_task: "asyncio.Task | None", raw: bytes) -> None:
        if prev_task is not None:
            # Wait for this partition's previous message to fully finish
            # (including its handler's own awaits) before starting this
            # one — this is what makes per-partition ordering hold even
            # though tasks are scheduled concurrently.
            await prev_task
        async with sem:
            await handler(raw)

    async for msg in consumer:
        tp = (msg.topic, msg.partition)
        prev_task = partition_chain.get(tp)
        task = asyncio.create_task(_run_in_order(prev_task, msg.value))
        partition_chain[tp] = task
        all_tasks.add(task)
        task.add_done_callback(all_tasks.discard)
        processed += 1

        if len(all_tasks) >= max_pending:
            await asyncio.wait(all_tasks, return_when=asyncio.FIRST_COMPLETED)

        if processed % commit_every == 0:
            if all_tasks:
                await asyncio.wait(all_tasks)
            await consumer.commit()
            # Every chain has now fully drained (we just awaited all of
            # them), so it's safe to drop the stale task references.
            partition_chain.clear()

    if all_tasks:
        await asyncio.wait(all_tasks)


async def get_total_consumer_lag(consumer: AIOKafkaConsumer) -> int:
    total_lag = 0
    try:
        assignment = consumer.assignment()
        for tp in assignment:
            position = await consumer.position(tp)
            highwater = consumer.highwater(tp)
            if highwater is not None and position is not None:
                total_lag += max(0, highwater - position)
    except Exception:
        pass
    return total_lag


async def update_consumer_lag_gauge(consumer: AIOKafkaConsumer, gauge, topic: str, group: str) -> None:
    """Sample approximate consumer lag (highwater - current position) for
    every partition currently assigned to this consumer and record it on a
    Prometheus Gauge. Called periodically from a background task in each
    consumer service, not on the per-message hot path.
    """
    try:
        assignment = consumer.assignment()
        if not assignment:
            return
        total_lag = 0
        for tp in assignment:
            position = await consumer.position(tp)
            highwater = consumer.highwater(tp)
            if highwater is not None and position is not None:
                total_lag += max(0, highwater - position)
        gauge.labels(topic=topic, group=group).set(total_lag)
    except Exception:
        # Lag reporting is best-effort observability; never let it crash
        # the consumer loop.
        pass


async def periodic_lag_reporter(
    consumer: AIOKafkaConsumer, gauge, topic: str, group: str, interval_seconds: float = 5.0
) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        await update_consumer_lag_gauge(consumer, gauge, topic, group)
