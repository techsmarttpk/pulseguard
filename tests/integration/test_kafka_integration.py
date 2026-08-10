"""Kafka integration test: produces and consumes a real message against a
running broker. Requires `make up` (or `docker compose up -d kafka
kafka-init`) to already be running — skipped automatically otherwise so
`pytest tests/` still passes in an environment with no Docker.
"""
import asyncio
import os
import time

import pytest

pytest.importorskip("aiokafka")

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # noqa: E402
from aiokafka.errors import KafkaConnectionError  # noqa: E402

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TEST_TOPIC = "pulseguard-test-roundtrip"


async def _kafka_reachable() -> bool:
    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, request_timeout_ms=3000)
    try:
        await producer.start()
        await producer.stop()
        return True
    except Exception:
        return False


@pytest.mark.asyncio
async def test_produce_and_consume_roundtrip():
    if not await _kafka_reachable():
        pytest.skip(f"Kafka not reachable at {BOOTSTRAP} — run `make up` first")

    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    await producer.start()

    consumer = AIOKafkaConsumer(
        TEST_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=f"pulseguard-test-{int(time.time())}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    await consumer.start()

    try:
        payload = b'{"hello": "pulseguard"}'
        await producer.send_and_wait(TEST_TOPIC, value=payload)

        received = None
        async with asyncio.timeout(10):
            async for msg in consumer:
                received = msg.value
                break

        assert received == payload
    finally:
        await producer.stop()
        await consumer.stop()
