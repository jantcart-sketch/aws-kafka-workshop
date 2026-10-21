# Extra Lab (optional) — Your own Python client: producer + consumer with analytics

> Not part of the time allotted to Labs 1 and 2 — the base agenda is already tight (see the root README). This lab is for anyone who finishes early, or wants to take it home to keep experimenting in their account after the workshop.
>
> Goal: move from "operating Kafka with the CLI" (Labs 1 and 2) to "writing an application that talks to Kafka" — the next layer, with its own learning curve (programmatic IAM auth, callback handling, serialization).

## Why this is a separate lab, not an extension of Lab 1/2

Labs 1 and 2 use ready-made tools (`kafka-console-producer.sh`, etc.) to teach the Kafka concepts themselves: partitions, offsets, replay, compaction. Here the goal changes — it's no longer about learning Kafka, it's about learning how a real application (in Python, in this case) connects, authenticates, and produces/consumes messages on its own. These are two different skills, which is why they live in separate documents.

## Part 1 — Producer: your own firehose

Already exists and is tested: `producer/firehose_producer.py`, at the repo root. Generates synthetic orders non-stop — meant to be left running for hours (the whole workshop, or longer) while you practice lag/replay against a stream that never stops.

**Important before starting it:** by default the script has no bootstrap string baked in — you must export your own `$BS` first (see below).

```bash
cd producer/   # or wherever you cloned/downloaded the repo
pip3 install --user -r requirements.txt   # first time only

export MSK_BOOTSTRAP_SERVERS="$BS"
export AWS_REGION="us-east-2"
export KAFKA_TOPIC="orders-live"      # or "orders", your choice
export PRODUCE_RATE_PER_SEC=5          # messages per second, adjustable

python3 firehose_producer.py
```

Leave it running in one tab (use `nohup ... &` if you want it to survive closing the SSM session — see the note at the end).

In another tab, consume and watch the lag grow and recover:

```bash
$KAFKA/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $PROPS \
  --topic orders-live --group firehose

$KAFKA/bin/kafka-consumer-groups.sh --bootstrap-server $BS --command-config $PROPS \
  --describe --group firehose
```

Disconnect the consumer for 1 minute while the producer keeps running, reconnect with the same `--group firehose`, and watch the `LAG` grow and then your consumer catch up without losing anything — the same principle from Lab 1, but now fed by a real application instead of you typing by hand.

## Part 2 — Consumer with live analytics (to be written)

The idea: a Python script that continuously consumes from `orders-live` (or `orders`) and aggregates something live — e.g. order count by country/channel, total amount, average ticket size — printing a summary every few seconds.

It would reuse the same IAM authentication block already solved and tested in `firehose_producer.py` (the `_oauth_cb` function and SASL/OAUTHBEARER config), swapping `Producer` for `Consumer` from `confluent-kafka`. Expected size: ~100-130 lines, similar to the producer.

**To be decided before writing it:** what level of analytics you want:
1. Just aggregate in memory and print to screen every few seconds (simplest).
2. Dump to CSV/JSON to analyze later with another tool.
3. Store it somewhere persistent (DB, or another aggregated topic) — this already looks like a mini streaming pipeline, with more scope.

## Part 3 — Avro producer/consumer with Glue Schema Registry (Level 2 of Lab 2 Part D)

> There's no official AWS library for Python as mature as the Java one (`aws-glue-schema-registry` is mostly Java/Kafka Streams). Here the **wire format** Glue expects is implemented by hand, confirmed against two independent sources (WarpStream docs + the official `awslabs/aws-glue-schema-registry` repo):
>
> ```
> byte 0       : 0x03   (magic byte, Glue wire format version)
> byte 1       : 0x00 = uncompressed, 0x05 = ZLIB-compressed
> bytes 2-17   : UUID (16 bytes) of the SchemaVersionId used to serialize
> bytes 18+    : the record's Avro bytes (uncompressed here)
> ```

Scripts: `producer/avro_producer.py` and `producer/avro_consumer.py` (repo root). They reuse the same IAM authentication pattern as `producer/firehose_producer.py` (the `_oauth_cb` function, SASL/OAUTHBEARER config) — not repeated here, see that file.

**Verified before touching AWS:** the wire format encode/decode (no boto3 or Kafka involved, just the byte logic) was tested locally with a throwaway script. Result: correct round-trip for v1 and v2 separately, and most importantly — a message written with schema v1 (no `channel`), read with schema v2 as the "reader schema", automatically reconstructs `channel: "unknown"` thanks to Avro's `default`. This demonstrates in real code the same `BACKWARD` compatibility seen in the Glue registry in Lab 2 Part D.

### How to run it

Additional dependency on top of what you already have:
```bash
pip3 install --user fastavro
```

Create the destination topic, if it doesn't exist yet (it was never pre-created alongside `orders`/`customers`):
```bash
$KAFKA/bin/kafka-topics.sh --bootstrap-server $BS --command-config $PROPS \
  --create --if-not-exists --topic orders-avro --partitions 1 --replication-factor 2
```

Produce 5 Avro messages (uses the `order-v2.avsc` schema by default, asking Glue for the `SchemaVersionId` corresponding to the schema registered in Lab 2 Part D — needs the `pandora-workshop` registry with the `order` schema to already exist):
```bash
export MSK_BOOTSTRAP_SERVERS="$BS"
export AWS_REGION="us-east-2"
cd producer/
python3 avro_producer.py
```

In another tab, consume and watch it decode correctly without being told which schema to use — it resolves that itself from each message's header:
```bash
export MSK_BOOTSTRAP_SERVERS="$BS"
export AWS_REGION="us-east-2"
cd producer/
python3 avro_consumer.py
```

**For the full compatibility demo:** run `avro_producer.py` once with `AVRO_SCHEMA_PATH=../schemas/order-v1.avsc` and once with `order-v2.avsc` (the default). The same `avro_consumer.py`, without restarting it or changing a single line of code, should read both message shapes correctly — that's the core point of this whole level: the consumer doesn't need to know in advance what shape each message has, it discovers it dynamically via Glue.

> **`avro_producer.py` is not a continuous stream like `firehose_producer.py`.** It produces exactly `NUM_MESSAGES` messages (default 5) and exits on its own — no infinite loop, no `Ctrl+C` needed. For more sample messages: `export NUM_MESSAGES=20` before running it.
>
> **Note on paths:** the script looks for the `.avsc` file relative to **where you run it from**, not where the script lives. If you `cd producer/` and the `.avsc` files live one level up (`~/schemas/`), pass the explicit path: `export AVRO_SCHEMA_PATH="../schemas/order-v2.avsc"`.
>
> **If you produce and the consumer sees nothing (topic at 0 messages despite a "Done: N produced")**, it's not a bug in these scripts — it's almost certainly the `set -euo pipefail` issue in `bootstrap.sh` described in `docs/attendee-guide.md` §1. Re-run `source bootstrap.sh` with the fixed script in both tabs before reproducing.

✅ **Verified end-to-end against the real cluster** (Oct 21, 2026): the producer serializes with Glue's v2 schema, Kafka accepts the message with the correct wire format, and `avro_consumer.py` reads and decodes it correctly by resolving the schema from the embedded `SchemaVersionId` — without the consumer having to know in advance what shape the message had.

## Note: keeping the producer alive beyond an SSM session

If you want the firehose to keep running even after you close the terminal (e.g. so it generates data all morning without depending on a tab staying open):

```bash
nohup python3 firehose_producer.py > firehose.log 2>&1 &
disown
```

To stop it later: `ps aux | grep firehose_producer` and `kill <PID>`. The repo's `producer/Dockerfile` is the more robust alternative if you want something that survives even a host reboot — not needed for this optional lab, but it's there if you need it.
