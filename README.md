# Apache Kafka on AWS — Workshop (Amazon MSK)

Hands-on workshop materials for learning Apache Kafka on Amazon MSK: a unified slide deck, two guided labs, an optional Python client lab, and the scripts you need on your workshop host.

Each attendee gets a dedicated AWS account for the event (via AWS Workshop Studio), with its own MSK cluster and a pre-provisioned client EC2 instance. You don't need to create any infrastructure — it's already there when you log in.

## Quick start

1. Open the deck: [`index.html`](index.html) (or open it locally in your browser). Toggle between **Workshop** (live session slides) and **Deep dive** (how Kafka works, internals) with the buttons at the top.
2. Follow the pre-work checklist: [`prework/checklist.md`](prework/checklist.md).
3. Connect to your workshop host (SSM Session Manager, no SSH needed) and run the setup script — see [`docs/attendee-guide.md`](docs/attendee-guide.md) section 1.
4. Work through the labs in order:
   - [`labs/lab1-first-event-stream.md`](labs/lab1-first-event-stream.md) — create a topic, produce/consume with keys, inspect a consumer group.
   - [`labs/lab2-replay-groups-schema-evolution.md`](labs/lab2-replay-groups-schema-evolution.md) — independent consumer groups, replay, compaction, Avro schema evolution with AWS Glue Schema Registry.
   - [`labs/lab-extra-python-client.md`](labs/lab-extra-python-client.md) (optional) — write your own Python producer/consumer, including one that speaks Avro + Glue Schema Registry.

## Repository layout

```
index.html                  Unified deck (Workshop + Deep dive, toggle at the top)
labs/                        Step-by-step lab guides
docs/                        Attendee command cheat sheet
prework/                     Pre-event checklist for attendees
scripts/                     bootstrap.sh / check-setup.sh / cleanup-topics.sh — run on your workshop host
producer/                    Python producer/consumer scripts (plain + Avro/Glue Schema Registry)
schemas/                     Avro schema examples (v1, v2 — backward-compatible evolution)
```

## What's already set up for you

- An MSK Provisioned cluster with **Express brokers**, 3 brokers, IAM authentication enabled.
- A client EC2 instance (`KafkaClientInstance`) with Java, the Kafka CLI, and the MSK IAM auth JAR already installed.
- Topics `orders`, `customers` (compacted), and `orders-avro` pre-created for you.

You connect to the client instance via **AWS Systems Manager Session Manager** — no SSH key, no public IP needed.

## Notes on this cluster (Express brokers)

A few Express-broker specifics you'll run into during the labs, documented in detail where they come up:

- Several broker-level configs (e.g. `segment.ms`) are managed by AWS and can't be edited on Express brokers. Compaction has its own fixed threshold instead — it triggers once a partition reaches 256 MB. With the small number of messages produced in the labs, you won't see real compaction happen on screen; the lab explains what *would* happen, out loud.
- Topic creation on this MSK Provisioned cluster requires passing `--replication-factor` explicitly (unlike MSK Serverless, which manages it for you).

See [`docs/attendee-guide.md`](docs/attendee-guide.md) for the full list of gotchas and their fixes, verified against a real cluster while building this workshop.
