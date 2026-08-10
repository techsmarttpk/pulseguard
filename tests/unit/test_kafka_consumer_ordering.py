"""Regression test for run_concurrent_consumer's per-partition ordering
guarantee (services/common/pulseguard_common/kafka_utils.py).

Found during a live-deployment investigation: the original implementation
fired every message into an unconstrained pool of concurrent asyncio
tasks with no ordering relationship between them. Every PulseGuard
consumer keeps *stateful*, order-sensitive per-symbol tracking (sequence
gaps, duplicates, rolling statistics), so two same-partition messages
finishing out of order corrupts that state and manufactures alerts/
anomalies that never actually happened in the stream — inflating alert
volume independent of anything really wrong with the feed. This test
deliberately makes an *earlier* message's handler call take longer than a
*later* one, which would reorder them under the old implementation, and
asserts the handler still observes them in Kafka's actual delivery order.
"""
import asyncio
from dataclasses import dataclass

import pytest

from pulseguard_common.kafka_utils import run_concurrent_consumer


@dataclass
class FakeRecord:
    topic: str
    partition: int
    value: bytes


class FakeConsumer:
    """Minimal stand-in for AIOKafkaConsumer: async-iterable over a fixed
    list of records, with a no-op commit()."""

    def __init__(self, records):
        self._records = records
        self.commit_calls = 0

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for record in self._records:
            yield record

    async def commit(self):
        self.commit_calls += 1


@pytest.mark.asyncio
async def test_same_partition_messages_processed_in_delivery_order():
    # Partition 0 gets 5 messages, 0-indexed by their intended order.
    # The FIRST message sleeps the longest and the rest get progressively
    # faster — under the old "fire and forget into N concurrent tasks"
    # design, message 0 would very likely finish *last*, not first.
    records = [FakeRecord(topic="market-data", partition=0, value=str(i).encode()) for i in range(5)]
    observed_order = []
    observed_lock = asyncio.Lock()

    async def handler(raw: bytes):
        i = int(raw.decode())
        # Earlier messages take longer to process than later ones.
        await asyncio.sleep(0.05 - i * 0.01)
        async with observed_lock:
            observed_order.append(i)

    consumer = FakeConsumer(records)
    await run_concurrent_consumer(consumer, handler, concurrency=4, commit_every=1000)

    assert observed_order == [0, 1, 2, 3, 4], (
        "messages from the same partition must be handled in Kafka's delivery "
        f"order regardless of individual handler durations; got {observed_order}"
    )


@pytest.mark.asyncio
async def test_different_partitions_process_concurrently_not_serially():
    # 2 partitions x 5 messages each, every handler call sleeps a fixed
    # 30ms. If partitions were (incorrectly) serialized against each
    # other, this would take ~10 * 30ms = 300ms. With real cross-partition
    # concurrency (bounded by concurrency=4) it should take meaningfully
    # less than that.
    records = []
    for p in range(2):
        for i in range(5):
            records.append(FakeRecord(topic="market-data", partition=p, value=f"{p}:{i}".encode()))

    async def handler(raw: bytes):
        await asyncio.sleep(0.03)

    consumer = FakeConsumer(records)
    start = asyncio.get_event_loop().time()
    await run_concurrent_consumer(consumer, handler, concurrency=4, commit_every=1000)
    elapsed = asyncio.get_event_loop().time() - start

    assert elapsed < 0.3, f"expected cross-partition concurrency to beat fully-serial time, took {elapsed:.3f}s"


@pytest.mark.asyncio
async def test_ordering_holds_within_each_partition_when_interleaved():
    # Interleave two partitions' records in delivery order, as Kafka's
    # consumer.poll() naturally would, and confirm each partition's
    # observed sub-sequence is still exactly in order even though the two
    # partitions' handlers race each other.
    records = []
    for i in range(6):
        records.append(FakeRecord(topic="market-data", partition=i % 2, value=f"{i % 2}:{i // 2}".encode()))

    observed = {0: [], 1: []}
    lock = asyncio.Lock()

    async def handler(raw: bytes):
        partition_str, seq_str = raw.decode().split(":")
        partition, seq = int(partition_str), int(seq_str)
        # Deliberately reverse timing bias so partition 0's *later*
        # messages resolve faster than its earlier ones.
        await asyncio.sleep(0.03 - seq * 0.01 if partition == 0 else 0.01)
        async with lock:
            observed[partition].append(seq)

    consumer = FakeConsumer(records)
    await run_concurrent_consumer(consumer, handler, concurrency=4, commit_every=1000)

    assert observed[0] == [0, 1, 2], observed
    assert observed[1] == [0, 1, 2], observed


@pytest.mark.asyncio
async def test_commit_called_after_batches_and_final_drain():
    records = [FakeRecord(topic="market-data", partition=0, value=str(i).encode()) for i in range(10)]

    async def handler(raw: bytes):
        return None

    consumer = FakeConsumer(records)
    await run_concurrent_consumer(consumer, handler, concurrency=4, commit_every=4)

    # 10 messages, commit_every=4 -> commits at message 4 and 8 (processed
    # % commit_every == 0), plus nothing extra needed at the end since all
    # tasks are awaited before returning either way.
    assert consumer.commit_calls == 2
