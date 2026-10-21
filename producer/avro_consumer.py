#!/usr/bin/env python3
"""
avro_consumer.py — Avro + AWS Glue Schema Registry consumer for `orders-avro`.

Companion to producer/avro_producer.py: reads messages serialized with
Glue Schema Registry's wire format, resolves the real schema from the
SchemaVersionId embedded in each message (via boto3's get_schema_version),
and deserializes the Avro bytes into a readable Python dict.

Central point of Lab 2 Part D: this same consumer, WITH NO CODE CHANGES,
can read messages produced with schema v1 (no 'channel') and v2 (with
'channel') interchangeably — because it resolves the correct schema
message by message from the header, instead of assuming a fixed shape.

Auth: same as producer/firehose_producer.py (SASL_SSL + OAUTHBEARER via
aws-msk-iam-sasl-signer-python).

Configuration via environment variables:
    MSK_BOOTSTRAP_SERVERS   (required) IAM bootstrap brokers string
    AWS_REGION              (required) region for the MSK cluster and the Glue registry
    KAFKA_TOPIC             source topic (default: "orders-avro")
    GROUP_ID                consumer group (default: "avro-lab-consumer")
    LOG_LEVEL               logging level (default: "INFO")

Usage:
    export MSK_BOOTSTRAP_SERVERS="$BS"
    export AWS_REGION="us-east-2"
    python3 avro_consumer.py

Stop: Ctrl+C.
"""

from __future__ import annotations

import io
import json
import logging
import os
import socket
import struct
import sys
import uuid

import boto3
import fastavro
from confluent_kafka import Consumer, KafkaException

LOG = logging.getLogger("avro_consumer")

GLUE_WIRE_FORMAT_MAGIC_BYTE = 3
GLUE_HEADER_SIZE = 18  # 1 (magic) + 1 (compression) + 16 (UUID)


def env_str(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def _oauth_cb(region: str):
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    def callback(_config_str: str):
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        return token, expiry_ms / 1000.0

    return callback


def build_consumer(bootstrap_servers: str, region: str, group_id: str) -> Consumer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": _oauth_cb(region),
        "client.id": f"avro-consumer-{socket.gethostname()}",
        "group.id": group_id,
        "auto.offset.reset": "earliest",
    }
    return Consumer(conf)


class GlueSchemaResolver:
    """Resolves SchemaVersionId -> parsed Avro schema, with an in-memory cache."""

    def __init__(self, region: str):
        self._glue = boto3.client("glue", region_name=region)
        self._cache: dict[str, dict] = {}

    def resolve(self, schema_version_id: str) -> dict:
        if schema_version_id in self._cache:
            return self._cache[schema_version_id]

        resp = self._glue.get_schema_version(SchemaVersionId=schema_version_id)
        definition = json.loads(resp["SchemaDefinition"])
        parsed = fastavro.parse_schema(definition)
        self._cache[schema_version_id] = parsed
        return parsed


def decode_glue_message(payload: bytes, resolver: GlueSchemaResolver) -> dict:
    if len(payload) < GLUE_HEADER_SIZE:
        raise ValueError(f"Message too short to have a Glue header: {len(payload)} bytes")

    magic_byte, compression = struct.unpack("BB", payload[0:2])
    if magic_byte != GLUE_WIRE_FORMAT_MAGIC_BYTE:
        raise ValueError(f"Unexpected magic byte: {magic_byte} (expected {GLUE_WIRE_FORMAT_MAGIC_BYTE})")
    if compression != 0:
        raise ValueError(f"This consumer doesn't support decompression (compression={compression})")

    schema_version_id = str(uuid.UUID(bytes=payload[2:18]))
    avro_bytes = payload[18:]

    schema = resolver.resolve(schema_version_id)
    record = fastavro.schemaless_reader(io.BytesIO(avro_bytes), schema)
    return {"schema_version_id": schema_version_id, "record": record}


def main() -> None:
    logging.basicConfig(
        level=getattr(logging, env_str("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    bootstrap_servers = env_str("MSK_BOOTSTRAP_SERVERS", required=True)
    region = env_str("AWS_REGION", required=True)
    topic = env_str("KAFKA_TOPIC", default="orders-avro")
    group_id = env_str("GROUP_ID", default="avro-lab-consumer")

    consumer = build_consumer(bootstrap_servers, region, group_id)
    resolver = GlueSchemaResolver(region)
    consumer.subscribe([topic])

    LOG.info("Listening on topic=%s group=%s (Ctrl+C to exit)...", topic, group_id)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                decoded = decode_glue_message(msg.value(), resolver)
                LOG.info(
                    "key=%s schema_version=%s record=%s",
                    msg.key().decode("utf-8") if msg.key() else None,
                    decoded["schema_version_id"],
                    decoded["record"],
                )
            except ValueError as exc:
                LOG.warning("Could not decode message at offset %s: %s", msg.offset(), exc)
    except KeyboardInterrupt:
        LOG.info("Stopping (Ctrl+C)...")
    finally:
        consumer.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        LOG.exception("The Avro consumer exited with an error")
        sys.exit(1)
