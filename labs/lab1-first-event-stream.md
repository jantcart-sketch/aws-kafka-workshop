# Lab 1 — Your first event stream

> Estimated time: 30 min. Before starting, confirm with `./check-setup.sh` that your environment is ready (see the attendee guide, "Step 0").
>
> Each attendee works in their own AWS account (Workshop Studio), with their own MSK cluster and their own workshop host. You don't share anything with anyone else — everything you do here is yours alone, so feel free to break things.

## Goal

By the end of this lab you will have done, with your own hands, the four basic Kafka operations: create a topic, produce with a key, consume, and inspect a consumer group. This isn't too different from what you already know about message queues — but you'll notice one key difference that is the heart of this whole workshop: **consuming doesn't delete anything**.

## Your topics

Already created in your account before the workshop:

- `orders` — 3 partitions
- `customers` — 1 partition, compacted (you'll use this in Lab 2)
- `orders-avro` — 1 partition (Lab 2)

## Step 0 — Verify you're connected

```bash
KAFKA=/home/ec2-user/kafka
PROPS=$KAFKA/config/client-iam.properties

$KAFKA/bin/kafka-topics.sh --bootstrap-server $BS --command-config $PROPS --list
```

You should see your `orders` topic in the list (along with internal topics starting with `__`). If you see nothing or it times out, stop — don't continue without this working.

## Step 1 — Describe your topic

```bash
$KAFKA/bin/kafka-topics.sh --bootstrap-server $BS --command-config $PROPS \
  --describe --topic orders
```

**What to look at:** you'll see `PartitionCount: 3` and a list of partitions (0, 1, 2), each with a `Leader` (which broker is in charge of that partition) and a list of `Replicas`. Keep this in mind: **a partition is an independent log**. Your topic isn't "one mailbox" — it's 3 mailboxes running in parallel.

## Step 2 — Produce with a key

You're going to send a few "orders" using different `customerId` values as the key.

```bash
$KAFKA/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $PROPS \
  --topic orders --property "parse.key=true" --property "key.separator=:"
```

This drops you into interactive mode (`>`). Paste these lines one at a time, pressing Enter after each:

```
cust-001:{"orderId":"o1","amount":42.0}
cust-002:{"orderId":"o2","amount":15.5}
cust-001:{"orderId":"o3","amount":9.9}
cust-003:{"orderId":"o4","amount":120.0}
cust-001:{"orderId":"o5","amount":33.3}
```

Exit with `Ctrl+C` when done.

**Before moving on, think about this:** `cust-001` appears 3 times. Which partition do you think those 3 records ended up in? (Hint: Kafka computes `hash(key) % num_partitions` — the same key always lands on the same partition.)

## Step 3 — Consume from the beginning

```bash
$KAFKA/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $PROPS \
  --topic orders --from-beginning --property print.key=true
```

You'll see all 5 records. Look at the order: the 3 `cust-001` records should come out in the same relative order you wrote them (because they're in the same partition, and ordering within a partition is guaranteed). The `cust-002` and `cust-003` records can appear interleaved with the `cust-001` ones — there's no ordering guarantee *across* partitions.

Exit with `Ctrl+C`.

**Now repeat the exact same command.** What happens? You see the same 5 messages again — because you didn't pass `--group`, each run of the console consumer is "new" and re-reads everything from the start with `--from-beginning`. This is intentional: **consuming never deletes or consumes the message from the topic**, very different from a classic queue where the consumer acks and the message disappears.

## Step 4 — Use a real consumer group

```bash
$KAFKA/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $PROPS \
  --topic orders --group lab1 --from-beginning
```

Let it run for 2-3 seconds and stop it with `Ctrl+C`. Now run the same command again, **without `--from-beginning`**:

```bash
$KAFKA/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $PROPS \
  --topic orders --group lab1
```

Nothing shows up (unless you produce something new in another window). Why? Because the `lab1` group already has a **saved offset** — it remembers how far it read last time, and continues from there. This is the key piece: the offset is a cursor that belongs to the *group*, not the topic.

## Step 5 — Inspect the consumer group

```bash
$KAFKA/bin/kafka-consumer-groups.sh --bootstrap-server $BS --command-config $PROPS \
  --describe --group lab1
```

**What to look at:** the `CURRENT-OFFSET` column (how far your group has read on each partition), `LOG-END-OFFSET` (how many messages exist total on that partition), and `LAG` (the difference — how much is left to read). If `LAG` is 0 on every partition, your group is fully caught up.

## Takeaways from this lab

- A partition is the unit of ordering, parallelism, and replication.
- The same key always goes to the same partition → guaranteed ordering for that key.
- Consuming **doesn't delete**. The data stays in the topic until it expires via retention.
- The offset is a cursor per **consumer group**, not per topic — that's why a new group re-reads everything and an existing group continues where it left off.

This is exactly what you'll exploit in Lab 2: since the data never disappears, you can "rewind" (replay) whenever you want.
