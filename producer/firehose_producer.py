#!/usr/bin/env python3
"""
firehose_producer.py — Producer for the `orders-live` firehose topic.

Continuously generates synthetic orders and produces them to an MSK topic
authenticated with IAM. Meant to run as a "safety net + extra activity":
attendees connect to `orders-live` with their own consumer group (read-only)
and see fan-out, lag, and replay against a live stream.

Auth: SASL_SSL + OAUTHBEARER, token generated with aws-msk-iam-sasl-signer-python
(see https://docs.aws.amazon.com/msk/latest/developerguide/configure-clients-for-iam-access-control.html).
Requires valid AWS credentials in the environment (profile, instance role,
or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_SESSION_TOKEN) with
kafka-cluster:Connect / WriteData / DescribeTopic permissions on the
`orders-live` topic.

Configuration via environment variables:

    MSK_BOOTSTRAP_SERVERS   (required) IAM bootstrap brokers string, e.g.
                            "b-1.xxx.kafka.us-east-2.amazonaws.com:9098,..."
    AWS_REGION              (required) the MSK cluster's region, e.g. "us-east-2"
    KAFKA_TOPIC             destination topic (default: "orders-live")
    PRODUCE_RATE_PER_SEC    messages per second (default: 5)
    ORDER_NUM_KEYS          number of distinct customer_id values used as key (default: 200)
    LOG_LEVEL               logging level (default: "INFO")

Usage:
    export MSK_BOOTSTRAP_SERVERS="b-1....:9098,b-2....:9098"
    export AWS_REGION="us-east-2"
    python3 firehose_producer.py

Stop: Ctrl+C (SIGINT) or SIGTERM — flushes before exiting.
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import socket
import sys
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import KafkaError, Producer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

LOG = logging.getLogger("firehose_producer")

# Sample data for generating realistic-looking orders, with no external
# file dependency (requirement: zero local data dependencies so the
# producer runs the same way in Docker, on EC2, or on a laptop).
PRODUCTS = [
    ("SKU-EARR-001", "Earrings – Sparkle Hoops", 39.0),
    ("SKU-RING-014", "Ring – Signature Band", 55.0),
    ("SKU-NECK-022", "Necklace – Charm Link", 65.0),
    ("SKU-BRAC-009", "Bracelet – Moments Chain", 49.0),
    ("SKU-WATC-003", "Watch – Classic Steel", 129.0),
    ("SKU-RING-031", "Ring – Stackable Pavé", 45.0),
    ("SKU-EARR-018", "Earrings – Studs Essence", 29.0),
    ("SKU-NECK-040", "Necklace – Infinity Heart", 59.0),
]
CHANNELS = ["web", "app", "store"]
COUNTRIES = ["ES", "DE", "SG", "DK", "NL"]


def env_str(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


@dataclass
class Settings:
    bootstrap_servers: str
    region: str
    topic: str
    rate_per_sec: float
    num_keys: int
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            bootstrap_servers=env_str("MSK_BOOTSTRAP_SERVERS", required=True),
            region=env_str("AWS_REGION", required=True),
            topic=env_str("KAFKA_TOPIC", default="orders-live"),
            rate_per_sec=float(env_str("PRODUCE_RATE_PER_SEC", default="5")),
            num_keys=env_int("ORDER_NUM_KEYS", 200),
            log_level=env_str("LOG_LEVEL", default="INFO"),
        )


def _oauth_cb(region: str):
    """confluent-kafka callback for SASL/OAUTHBEARER.

    confluent-kafka calls this callback with the `sasl.oauthbearer.config`
    string (unused here) and expects back (token, expiry_epoch_seconds).
    """

    def callback(_config_str: str):
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        # The library returns expiry in epoch milliseconds; confluent-kafka
        # wants epoch seconds.
        return token, expiry_ms / 1000.0

    return callback


def build_producer(settings: Settings) -> Producer:
    conf = {
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": _oauth_cb(settings.region),
        "client.id": f"firehose-producer-{socket.gethostname()}",
        # Reliability: wait for acknowledgement from all in-sync replicas and
        # let the library retry before giving up on a message.
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
        "retry.backoff.ms": 500,
        "linger.ms": 50,
    }
    return Producer(conf)


def make_order(rng: random.Random, num_keys: int) -> tuple[str, dict[str, Any]]:
    """Generates (key, value) for a synthetic order.

    key = customer_id (so the same customer always lands on the same
    partition, illustrating the key-based partitioning covered in the deck).
    """
    customer_id = f"cust-{rng.randint(1, num_keys):05d}"
    sku, name, base_price = rng.choice(PRODUCTS)
    quantity = rng.randint(1, 3)
    amount = round(base_price * quantity * rng.uniform(0.95, 1.05), 2)

    order = {
        "orderId": str(uuid.uuid4()),
        "customerId": customer_id,
        "sku": sku,
        "productName": name,
        "quantity": quantity,
        "amount": amount,
        "channel": rng.choice(CHANNELS),
        "country": rng.choice(COUNTRIES),
        "ts": time.time(),
    }
    return customer_id, order


def delivery_report(err: KafkaError | None, msg) -> None:
    if err is not None:
        LOG.warning("Failed to deliver message key=%s: %s", msg.key(), err)


@dataclass
class Stats:
    sent: int = 0
    failed: int = 0
    last_report: float = field(default_factory=time.time)

    def maybe_report(self, interval_sec: float = 30.0) -> None:
        now = time.time()
        if now - self.last_report >= interval_sec:
            LOG.info("Progress: %d sent, %d failed", self.sent, self.failed)
            self.last_report = now


def run(settings: Settings) -> None:
    producer = build_producer(settings)
    rng = random.Random()
    stats = Stats()
    stop = {"flag": False}

    def handle_signal(signum, _frame):
        LOG.info("Received signal %s, stopping after the next flush...", signum)
        stop["flag"] = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    period = 1.0 / settings.rate_per_sec if settings.rate_per_sec > 0 else 0
    LOG.info(
        "Starting firehose: topic=%s rate=%.2f msg/s keys=%d bootstrap=%s",
        settings.topic,
        settings.rate_per_sec,
        settings.num_keys,
        settings.bootstrap_servers,
    )

    while not stop["flag"]:
        key, order = make_order(rng, settings.num_keys)
        try:
            producer.produce(
                topic=settings.topic,
                key=key.encode("utf-8"),
                value=json.dumps(order).encode("utf-8"),
                on_delivery=delivery_report,
            )
            stats.sent += 1
        except BufferError:
            LOG.warning("Local queue full, polling extra to free up space")
            producer.poll(0.5)
            continue
        except Exception:  # noqa: BLE001 — log and keep running for the full 3h
            LOG.exception("Unexpected error producing message")
            stats.failed += 1

        producer.poll(0)  # trigger any pending delivery callbacks
        stats.maybe_report()

        if period:
            time.sleep(period)

    LOG.info("Flushing...")
    producer.flush(30)
    LOG.info("Done: %d sent, %d failed", stats.sent, stats.failed)


def main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        run(settings)
    except Exception:
        LOG.exception("The producer exited with an unrecoverable error")
        sys.exit(1)


if __name__ == "__main__":
    main()
