# Lab 2 — Replay, groups, compaction, and schema evolution

> Estimated time: 35 min. Requires having completed Lab 1 (your `orders` topic should already have a few messages).

## Goal

Four ideas, each building on the previous one:
1. Two groups can read the same topic completely independently.
2. You can "rewind" a group to an earlier point (replay) — something impossible in a classic queue.
3. A compacted topic behaves like a table with a primary key, not an event log.
4. A schema can evolve without breaking old consumers.

## Part A — Two independent groups

Open two separate consumer sessions (if you're working in a single terminal, launch one, check the result, close it with `Ctrl+C`, then launch the other):

```bash
KAFKA=/home/ec2-user/kafka
PROPS=$KAFKA/config/client-iam.properties

# Group A
$KAFKA/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $PROPS \
  --topic orders --group lab2a --from-beginning

# Group B
$KAFKA/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $PROPS \
  --topic orders --group lab2b --from-beginning
```

Both read exactly the same messages, each with its own offset. **This is impossible to get "for free" with a classic queue** — there, if two consumers compete for the same message, one gets it and the other doesn't see it. Here each group has its own full copy of the stream.

## Part B — Replay (the magic of the retained log)

Let's make Group A "forget" what it's read and go back to the start.

```bash
# First, check Group A's current offset
$KAFKA/bin/kafka-consumer-groups.sh --bootstrap-server $BS --command-config $PROPS \
  --describe --group lab2a

# Reset Group A's offset to the beginning (the group must be inactive,
# with no consumer running, to be able to reset)
$KAFKA/bin/kafka-consumer-groups.sh --bootstrap-server $BS --command-config $PROPS \
  --group lab2a --topic orders \
  --reset-offsets --to-earliest --execute

# Consume again with Group A (no --from-beginning this time)
$KAFKA/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $PROPS \
  --topic orders --group lab2a
```

You'll see all the messages again, even though that same group had already read them before. **This is the real-world use case:** your application has a bug, you fix it, and instead of losing the data it processed incorrectly, you simply rewind the consumer and reprocess it. None of this requires touching the producer or re-sending anything — the data never left.

You can also reset to a specific point instead of the beginning:
```bash
# To a specific timestamp (partial replay)
$KAFKA/bin/kafka-consumer-groups.sh --bootstrap-server $BS --command-config $PROPS \
  --group lab2a --topic orders \
  --reset-offsets --to-datetime 2026-10-23T10:00:00.000 --execute
```

## Part C — Compaction (topic as table)

> ⚠️ **Important for this cluster (Express brokers):** on Express brokers, Amazon MSK manages `segment.ms`/`segment.bytes` for you and they **can't be edited** (trying gives `InvalidRequestException: Express brokers do not allow writing the following configuration: segment.ms`). Instead, Express has its own fixed rule: **compaction triggers once a partition accumulates 256 MB of data.** With the 4 test messages in this lab (a few hundred bytes total), you'll never reach those 256 MB — so **you won't actually see compaction happen in this demo**, and that's expected, not a failure. We still do the exercise to see the behavior *before* compaction and explain what would happen with real volume.
>
> (If you were on an MSK Standard cluster, `segment.ms` would be editable and you could force fast compaction with little data — but this workshop runs on Express.)

If it wasn't pre-created before the workshop, first create the `customers` topic with `cleanup.policy=compact` (so Kafka doesn't delete by time/retention, but by **key**: it keeps only the latest value per key):

```bash
$KAFKA/bin/kafka-topics.sh --bootstrap-server $BS --command-config $PROPS \
  --create --if-not-exists --topic customers --partitions 1 --replication-factor 2 \
  --config cleanup.policy=compact
```

(`--if-not-exists` avoids an error if it was already created.)

Now produce several versions of the same customer:

```bash
$KAFKA/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $PROPS \
  --topic customers --property "parse.key=true" --property "key.separator=:"
```

Paste these lines:
```
cust-001:{"name":"Maria","tier":"bronze"}
cust-001:{"name":"Maria","tier":"silver"}
cust-001:{"name":"Maria","tier":"gold"}
cust-002:{"name":"Pedro","tier":"bronze"}
```

Exit with `Ctrl+C`. Now consume from the beginning:

```bash
$KAFKA/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $PROPS \
  --topic customers --from-beginning --property print.key=true
```

You'll see all **4 lines** — and on this cluster they'll stay that way, uncompacted, because of the 256 MB threshold explained above. **Here's what you need to explain out loud, without being able to show it on screen:**

> "If this partition accumulated 256 MB of data — meaning far more messages than what we just wrote — Kafka would run compaction in the background and, when consuming from the beginning again, we'd only see **2 lines**: `cust-001` with `tier: gold` (the most recent value) and `cust-002` with `tier: bronze`. The intermediate versions of `cust-001` (`bronze`, `silver`) would have been removed, because compaction keeps only the latest value per key."

**Tombstones (concept, same reasoning):** if you produced a message with `value=null` for a key (leave the value empty after the colon, e.g. `cust-002:`), that's a signal to "delete this key". After a real compaction, that key would disappear from the topic. You can produce it anyway to see it doesn't error out, although — for the same reason as above — you won't see the deletion effect in this demo.

**Takeaway:** a normal topic is an event log (everything stays, ordered by time). A compacted topic is more like a table with upsert-by-key — useful for "current state of an entity" rather than "history of what happened". The 256 MB threshold on Express is a good example of this broker type's philosophy: fewer knobs to turn, in exchange for fixed rules optimized by AWS.

## Part D — Schema evolution with Avro + Glue Schema Registry

The reference schemas are in `schemas/order-v1.avsc` and `schemas/order-v2.avsc` in this repo.

1. **`order-v1.avsc`:** an `Order` with `orderId` (string) and `amount` (double).
2. **`order-v2.avsc`:** adds `channel` (string) with `"default": "unknown"`.

The key idea: v2 adds a field **with a default**, and that's what makes it backward-compatible (`BACKWARD`). If v2 had added a field *without* a default, or had deleted/renamed an existing field, Glue Schema Registry would reject it when trying to register it under `BACKWARD` compatibility.

This part has two levels. **Level 1** is pure AWS CLI — fast, demonstrates the compatibility concept without touching Kafka. **Level 2** (producing/consuming real Avro messages) requires a programmatic client because there's no `kafka-console-producer.sh` that serializes Avro against Glue — see `labs/lab-extra-python-client.md`, where it's combined with the Python client from the Extra lab.

### Level 1 — Registry and compatibility (CLI, do it yourself)

**1. Create the registry and register v1 at the same time** (`create-schema` creates the schema *and* its first version in one step):

```bash
aws glue create-registry --registry-name pandora-workshop --region us-east-2

aws glue create-schema \
  --registry-id RegistryName=pandora-workshop \
  --schema-name order \
  --data-format AVRO \
  --compatibility BACKWARD \
  --schema-definition "$(cat schemas/order-v1.avsc)" \
  --region us-east-2
```

**2. Register v2** (add a new version to the existing schema):

```bash
aws glue register-schema-version \
  --schema-id SchemaName=order,RegistryName=pandora-workshop \
  --schema-definition "$(cat schemas/order-v2.avsc)" \
  --region us-east-2
```

This should be accepted without error — check the status:
```bash
aws glue get-schema-by-definition \
  --schema-id SchemaName=order,RegistryName=pandora-workshop \
  --schema-definition "$(cat schemas/order-v2.avsc)" \
  --region us-east-2
```

**3. First attempt (a pedagogical surprise): deleting a field — this IS compatible.**

```bash
cat > /tmp/order-v3-no-amount.avsc <<'EOF'
{
  "type": "record",
  "name": "Order",
  "namespace": "com.pandora.workshop.kafka",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "channel", "type": "string", "default": "unknown" }
  ]
}
EOF

aws glue register-schema-version \
  --schema-id SchemaName=order,RegistryName=pandora-workshop \
  --schema-definition "$(cat /tmp/order-v3-no-amount.avsc)" \
  --region us-east-2
```

**This gives `Status: AVAILABLE`, not `FAILURE`** — and that's the correct result, even though it's counterintuitive. The actual rule for `BACKWARD` compatibility is: *"a consumer with the new schema must be able to read data written with the previous schema"*. If the `amount` field is no longer in the new schema, the new consumer simply **ignores** it when it shows up in old data — it doesn't need it, so there's no conflict. **Deleting a field is backward-compatible.**

**4. Second attempt: what actually breaks `BACKWARD` — adding a required field with no default.**

```bash
cat > /tmp/order-v4-required-taxid.avsc <<'EOF'
{
  "type": "record",
  "name": "Order",
  "namespace": "com.pandora.workshop.kafka",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "amount", "type": "double" },
    { "name": "channel", "type": "string", "default": "unknown" },
    { "name": "taxId", "type": "string" }
  ]
}
EOF

aws glue register-schema-version \
  --schema-id SchemaName=order,RegistryName=pandora-workshop \
  --schema-definition "$(cat /tmp/order-v4-required-taxid.avsc)" \
  --region us-east-2
```

**This should give `Status: FAILURE`.** Reason: `taxId` has no `"default"`. The new consumer needs it to read the record, but old data (v1/v2/v3) never had that field — there's no fallback value to use, so reading would fail. Confirm with:

```bash
aws glue list-schema-versions \
  --schema-id SchemaName=order,RegistryName=pandora-workshop \
  --region us-east-2
```

You should see 4 versions: v1, v2, and v3 as `AVAILABLE`, and v4 as `FAILURE`.

**Takeaway from this specific experiment:** the intuition of "deleting a field = dangerous, adding a field = safe" is backwards relative to `BACKWARD` compatibility. What actually matters is whether the *new* consumer can handle *old* data — deleting something you no longer need never breaks that; requiring something new that old data doesn't have, with no fallback value, does break it.

> ✅ **Verified against the real cluster/registry** (Oct 21, 2026): v3 without `amount` → `AVAILABLE` as predicted; v4 with required `taxId` → `FAILURE` as predicted. The first version of this lab incorrectly assumed deleting a field would break compatibility — corrected after testing it live and getting `AVAILABLE` instead of `FAILURE`.

### Level 2 — Producing/consuming real Avro messages

This is no longer CLI — it needs a Python client with `boto3` + an Avro+Glue serialization library. Documented together with the Python client in the Extra lab at `labs/lab-extra-python-client.md`, reusing the same IAM authentication base already proven in that lab's producer.

**Takeaway (valid even with only Level 1 done):** a topic's schema isn't static. You can evolve it safely by following compatibility rules (add fields with a default, never delete/rename without a migration plan) — and the registry protects you from breaking things by accident, rejecting the version before it reaches production.

## Extra — Got time left?

The "your own firehose + Python analytics" content moved to its own document: **`labs/lab-extra-python-client.md`**. It's optional, not part of the time allotted to Lab 2 — do it if you finish early or want to take it home to try after the workshop.

## Workshop summary

| What you saw | Why it matters |
|---|---|
| Same key → same partition | Guaranteed ordering per entity, with no extra coordination |
| Consuming doesn't delete | You can have N independent consumers of the same stream |
| Offset belongs to the group, not the topic | Replay is free: you're just moving a pointer |
| Compaction | A topic can be "current state", not just "history" |
| Schema evolution | You can change the data contract without stopping production or consumers |
