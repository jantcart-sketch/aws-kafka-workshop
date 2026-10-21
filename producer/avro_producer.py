#!/usr/bin/env python3
"""
avro_producer.py — Avro + AWS Glue Schema Registry producer for `orders-avro`.

Companion to Lab 2 Part D (Level 2): produces Avro-serialized orders using a
schema already registered in Glue Schema Registry (see the AWS CLI commands
in Lab 2 — this script assumes the "order" registry and schema already
exist, with at least v1 or v2 registered).

There's no official AWS library for Python as complete as the Java one
(aws-glue-schema-registry is mostly Java/Kafka Streams). Here the wire
format Glue expects is implemented by hand, documented at:
https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html
and confirmed by a second source (WarpStream docs, "For AWS Glue, the
encoding starts off with a magic byte of 3, followed by one byte which
denotes whether the data is compressed and 16 bytes of UUID..."):

    byte 0       : 0x03 (magic byte, Glue wire format version)
    byte 1       : 0x00 = uncompressed, 0x05 = ZLIB-compressed
    bytes 2-17   : UUID (16 bytes) of the SchemaVersionId used to serialize
    bytes 18+    : the record's Avro bytes (uncompressed here)

Auth: same as producer/firehose_producer.py (SASL_SSL + OAUTHBEARER via
aws-msk-iam-sasl-signer-python) — same pattern, not re-explained here.

Configuration via environment variables:
    MSK_BOOTSTRAP_SERVERS   (required) IAM bootstrap brokers string
    AWS_REGION              (required) region for the MSK cluster and the Glue registry
    KAFKA_TOPIC             destination topic (default: "orders-avro")
    GLUE_REGISTRY_NAME      Glue registry name (default: "pandora-workshop")
    GLUE_SCHEMA_NAME        Glue schema name (default: "order")
    NUM_MESSAGES            how many messages to produce then exit (default: 5)
    LOG_LEVEL               logging level (default: "INFO")

Usage:
    export MSK_BOOTSTRAP_SERVERS="$BS"
    export AWS_REGION="us-east-2"
    python3 avro_producer.py
"""

from __future__ import annotations

import io
import json
import logging
import os
import socket
import struct
import sys
import time
import uuid
from dataclasses import dataclass

import boto3
import fastavro
from confluent_kafka import KafkaError, Producer

LOG = logging.getLogger("avro_producer")

GLUE_WIRE_FORMAT_MAGIC_BYTE = 3
GLUE_WIRE_FORMAT_NO_COMPRESSION = 0


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
    registry_name: str
    schema_name: str
    num_messages: int
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            bootstrap_servers=env_str("MSK_BOOTSTRAP_SERVERS", required=True),
            region=env_str("AWS_REGION", required=True),
            topic=env_str("KAFKA_TOPIC", default="orders-avro"),
            registry_name=env_str("GLUE_REGISTRY_NAME", default="pandora-workshop"),
            schema_name=env_str("GLUE_SCHEMA_NAME", default="order"),
            num_messages=env_int("NUM_MESSAGES", 5),
            log_level=env_str("LOG_LEVEL", default="INFO"),
        )


def _oauth_cb(region: str):
    """Identical to the one in producer/firehose_producer.py — see that file for more detail."""
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    def callback(_config_str: str):
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        return token, expiry_ms / 1000.0

    return callback


def build_producer(settings: Settings) -> Producer:
    conf = {
        "bootstrap.servers": settings.bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": _oauth_cb(settings.region),
        "client.id": f"avro-producer-{socket.gethostname()}",
        "acks": "all",
        "enable.idempotence": True,
    }
    return Producer(conf)


class GlueSchemaClient:
    """Thin wrapper over boto3 to resolve schema <-> SchemaVersionId.

    Caches in memory (a SchemaVersionId never changes once registered) so
    we don't call Glue on every produced message.
    """

    def __init__(self, region: str, registry_name: str, schema_name: str):
        self._glue = boto3.client("glue", region_name=region)
        self._registry_name = registry_name
        self._schema_name = schema_name
        self._version_id_cache: dict[str, str] = {}

    def get_schema_version_id(self, schema_definition: str) -> str:
        if schema_definition in self._version_id_cache:
            return self._version_id_cache[schema_definition]

        resp = self._glue.get_schema_by_definition(
            SchemaId={
                "SchemaName": self._schema_name,
                "RegistryName": self._registry_name,
            },
            SchemaDefinition=schema_definition,
        )
        version_id = resp["SchemaVersionId"]
        self._version_id_cache[schema_definition] = version_id
        return version_id


def encode_with_glue_header(record: dict, avro_schema: dict, schema_version_id: str) -> bytes:
    """Serializes `record` as Avro and prepends Glue's wire format header.

    Format (see module docstring): 1 magic byte (0x03) + 1 compression byte
    (0x00 = uncompressed) + 16 bytes of UUID + Avro bytes.
    """
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, avro_schema, record)
    avro_bytes = buf.getvalue()

    header = struct.pack(
        "BB",
        GLUE_WIRE_FORMAT_MAGIC_BYTE,
        GLUE_WIRE_FORMAT_NO_COMPRESSION,
    ) + uuid.UUID(schema_version_id).bytes

    return header + avro_bytes


def delivery_report(err: KafkaError | None, msg) -> None:
    if err is not None:
        LOG.warning("Failed to deliver message: %s", err)
    else:
        LOG.info("Delivered to %s [partition %s] offset %s", msg.topic(), msg.partition(), msg.offset())


def load_avro_schema(path: str) -> tuple[dict, str]:
    """Loads a .avsc file and returns (schema_parsed_for_fastavro, raw_text)."""
    with open(path, "r", encoding="utf-8") as fh:
        raw = fh.read()
    schema = fastavro.parse_schema(json.loads(raw))
    return schema, raw


def main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Uses the v2 schema (with 'channel') by default — see schemas/order-v2.avsc.
    # Change the path here if you want to explicitly test v1.
    schema_path = os.environ.get("AVRO_SCHEMA_PATH", "schemas/order-v2.avsc")
    avro_schema, raw_schema_text = load_avro_schema(schema_path)

    glue = GlueSchemaClient(settings.region, settings.registry_name, settings.schema_name)
    schema_version_id = glue.get_schema_version_id(raw_schema_text)
    LOG.info("Using schema %s -> SchemaVersionId=%s", schema_path, schema_version_id)

    producer = build_producer(settings)

    for i in range(settings.num_messages):
        record = {
            "orderId": str(uuid.uuid4()),
            "amount": round(10 + i * 7.5, 2),
            "channel": ["web", "app", "store"][i % 3],
        }
        payload = encode_with_glue_header(record, avro_schema, schema_version_id)

        producer.produce(
            topic=settings.topic,
            key=f"cust-{i:03d}".encode("utf-8"),
            value=payload,
            on_delivery=delivery_report,
        )
        producer.poll(0)
        LOG.info("Produced: %s", record)
        time.sleep(0.2)

    producer.flush(30)
    LOG.info("Done: %d messages produced to %s", settings.num_messages, settings.topic)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOG.exception("The Avro producer exited with an error")
        sys.exit(1)
