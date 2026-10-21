# Attendee guide — Kafka on AWS Workshop

> Quick reference guide for following the labs live. This doesn't replace the deck — it's the "command sheet" to keep next to you as the instructor moves through the material.
>
> Workshop Studio model: you have your own **isolated AWS account** for the workshop, with your own MSK cluster and your own workshop host already deployed. You don't share anything with other attendees — there's no `<username>` to compute and no topic suffixes, your topics are simply called `orders`, `customers`, `orders-avro`.

## 0 · Accessing your account

Sign in with the Event Hash / Team Hash the instructor gave you at [Workshop Studio](https://catalog.workshops.aws) (or whatever portal is noted in the pre-work). You'll see temporary AWS credentials and access to your workshop host.

## 1 · Connecting (Step 0)

> ⚠️ **Very important — read this before continuing.** You'll need **two SSM tabs/sessions open at the same time** against your `KafkaClientInstance` (one to produce, one to consume, since the consumer sits "hanging" waiting for messages). Each tab is a **completely independent connection**: they share nothing with each other. This means that in **every new tab** you open, you have to repeat, in this order, the following two things — it's not optional, and it's not `Ctrl+C`'s fault:
>
> ```bash
> sudo su - ec2-user
> source bootstrap.sh
> ```
>
> `Ctrl+C` by itself (to stop a consumer or producer) **does not** clear `$BS` or kick you out of `ec2-user` — your session stays intact. What resets everything is opening a **new tab/session**, because each one starts from scratch as `ssm-user`, with no memory of what you did in another one. If any command gives you `Invalid url in bootstrap.servers`, 90% of the time this is why: you're in a tab where you haven't yet run `sudo su` + `source bootstrap.sh`.
>
> ⚠️ **Another real risk, already fixed in `scripts/bootstrap.sh` but documented here in case you're using an old copy:** if your `bootstrap.sh` has a `set -euo pipefail` line at the top, **remove it**. Since the script is `source`d (run inside your own session, not as a separate process), those options stay active in your terminal forever after loading it — not just while the script runs. The symptom: any normal command that fails, or any unexported variable (`echo "$SOME_UNDEFINED_VAR"` with `-u` active), **kills your `ec2-user` session outright** and drops you back to the `ssm-user` prompt without warning, losing `$BS` and everything you had loaded. If your prompt changes on its own (from `KafkaClientEC2Instance1 [ec2-user@...]$` to something like `sh-5.2$`), this is what happened — run `sudo su - ec2-user` + `source bootstrap.sh` again.

Open your two tabs now, and in **each** of them do:

```bash
sudo su - ec2-user
source bootstrap.sh
```

Then, in either tab, verify you can see the topic list:

```bash
/home/ec2-user/kafka/bin/kafka-topics.sh --bootstrap-server $BS --command-config /home/ec2-user/kafka/config/client-iam.properties --list
```

If this fails, run `./check-setup.sh` to see exactly which check is broken.

From here on, throughout this guide: **"Tab A" = where you produce, "Tab B" = where you consume.**

## 2 · Lab 1 — First event stream

```bash
KAFKA=/home/ec2-user/kafka
PROPS=$KAFKA/config/client-iam.properties

# Describe your topic (either tab)
$KAFKA/bin/kafka-topics.sh --bootstrap-server $BS --command-config $PROPS \
  --describe --topic orders

# --- Tab A: produce with a key (format: key:value, default separator ':') ---
$KAFKA/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $PROPS \
  --topic orders --property "parse.key=true" --property "key.separator=:"
> cust-001:{"orderId":"o1","amount":42.0}
> cust-002:{"orderId":"o2","amount":15.5}
> cust-001:{"orderId":"o3","amount":9.9}

# --- Tab B: consume from the beginning ---
$KAFKA/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $PROPS \
  --topic orders --from-beginning --property print.key=true

# Inspect your consumer group (offsets, lag) — either tab
$KAFKA/bin/kafka-consumer-groups.sh --bootstrap-server $BS --command-config $PROPS \
  --describe --group lab1
```

Remember: if Tab B is new (you haven't opened it yet in this session), first `sudo su - ec2-user` and `source bootstrap.sh` there too — see the section 1 warning.

**Key lesson:** `cust-001` always lands on the same partition → guaranteed ordering for that customer. A new consumer group re-reads everything from the start if you use `--from-beginning`; consuming never deletes anything from the topic.

## 3 · Lab 2 — Replay, groups, evolution

```bash
KAFKA=/home/ec2-user/kafka
PROPS=$KAFKA/config/client-iam.properties

# Two independent groups reading the same topic
$KAFKA/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $PROPS \
  --topic orders --group lab2a

$KAFKA/bin/kafka-console-consumer.sh --bootstrap-server $BS --consumer.config $PROPS \
  --topic orders --group lab2b

# Replay: reset a group's offsets to the beginning
$KAFKA/bin/kafka-consumer-groups.sh --bootstrap-server $BS --command-config $PROPS \
  --group lab2a --topic orders \
  --reset-offsets --to-earliest --execute

# Compaction: produce several versions of the same key in customers
# (compacted topic — Kafka keeps the latest value per key; value=null is a tombstone)
$KAFKA/bin/kafka-console-producer.sh --bootstrap-server $BS --producer.config $PROPS \
  --topic customers --property "parse.key=true" --property "key.separator=:"
> cust-001:{"name":"Maria","tier":"gold"}
> cust-001:{"name":"Maria","tier":"platinum"}
```

## 4 · Extra — Your own firehose

Each account is independent, so the firehose isn't shared: each attendee can run their own instance of `producer/firehose_producer.py` against their own cluster to generate a continuous stream and practice lag/replay. See `labs/lab2-replay-groups-schema-evolution.md`, "Extra" section.

## 5 · Common errors

| Symptom | Likely cause |
|---|---|
| `TopicAuthorizationException` | Your IAM role doesn't have permission on that topic pattern, or you mistyped the name (check casing/characters) |
| Timeout connecting | `$BS` isn't exported in this terminal, or the SG doesn't allow 9098 from your host |
| Consumer shows nothing | Missing `--from-beginning` (a new group starts at the end by default) or you're on the wrong topic/partition |
| `INVALID_REQUEST` creating a topic | On this Provisioned cluster you must pass `--replication-factor` explicitly, unlike MSK Serverless. Note: on Express brokers the actual topic always comes back as `ReplicationFactor: 3` no matter what number you pass — that's expected AWS behavior for Express, not an error. |
| `Topic 'Optional[orders]' does not exist as expected` (or `customers`/`orders-avro`) | `source bootstrap.sh` creates these three topics automatically (idempotently) every time it runs — if you see this, you likely skipped `source bootstrap.sh` in this tab, or it failed silently earlier in the script (scroll up its output). Re-run `source bootstrap.sh`, then `./check-setup.sh` to confirm all three topics are now present. |
| `Invalid url in bootstrap.servers: --command-config` (or any other flag showing up as if it were the URL) | `$BS` is empty in this terminal session. **Most common cause: this is a new SSM tab/session where you haven't yet run `sudo su - ec2-user` + `source bootstrap.sh`.** Each tab is independent — they don't share variables, even on the same host. `Ctrl+C` is not the cause; opening a new tab is. Check with `echo "BS=$BS"` — if empty, repeat the two commands from section 1 in that tab. |
| `WARN ... The DescribeTopicPartitions API is not supported, using Metadata API to describe topics` when using `kafka-topics.sh` or `kafka-consumer-groups.sh` | **Not an error, purely informational.** The host's Kafka client (more recent) tries to use a newer API that this MSK broker version doesn't support yet, and automatically falls back to the older API to get the same information. The command still works fine — you'll see this warning regularly, ignore it. |
| `WARN ... Error while fetching metadata ... UNKNOWN_TOPIC_OR_PARTITION` repeating in a loop while producing | **This one IS a real error** (unlike the one above): the topic you're trying to produce to doesn't exist yet. Mostly happens with `customers` and `orders-avro` in Lab 2 if they weren't pre-created before the workshop. Exit with `Ctrl+C` and create it (see `labs/lab2-replay-groups-schema-evolution.md` Part C for the full `customers` command, including the compaction config). |
| `InvalidRequestException: Express brokers do not allow writing the following configuration: segment.ms` when trying `kafka-configs.sh --alter` | **Expected, not a bug.** On Express brokers, AWS manages `segment.ms`/`segment.bytes` and they can't be edited. Compaction on Express has its own fixed threshold instead: it triggers once a partition reaches **256 MB**, with no configuration possible. With the small number of messages in Lab 2, that threshold is never reached — that's why Lab 2 Part C explains the expected result out loud instead of demonstrating it on screen. Don't try to force it with `--alter`, it will always fail on this cluster. |
| A `register-schema-version` that deletes a field gives `Status: AVAILABLE` instead of `FAILURE` (you expected it to fail) | **Not an error — your intuition about `BACKWARD` is what's off.** `BACKWARD` compatibility means the new consumer must be able to read old data. Deleting a field is safe (the new consumer simply ignores it if it shows up). What actually breaks `BACKWARD` is adding a **required field with no `default`** — there the new consumer needs a value that old data never had. See Lab 2 Part D, verified against the real registry. |
| `avro_producer.py` ends with `%4\|...\|TERMINATE\|...Producer terminating with N messages (...) still in queue or transit: use flush()` right after seeing "Done: N messages produced" | **Cosmetic, doesn't indicate data loss**, as long as you saw the `Delivered to orders-avro [partition ...] offset ...` lines for each message beforehand (that confirms the broker already accepted them). It's an internal `confluent-kafka` notice when destroying the `Producer` object as the process exits. If instead you see NO "Delivered" lines before "Done", that WOULD be a real delivery failure — check topic connectivity and permissions. |
| You produced messages and the consumer shows nothing, and `kafka-get-offsets.sh --topic orders-avro` says `orders-avro:0:0` (0 messages) even though the producer said "Done: 5 messages produced" | Symptom of the `set -euo pipefail` bug above: your `ec2-user` session got killed outright at some point in between (by a failing command or an unexported variable), and without noticing you ended up operating in a different context than you thought — you likely produced with a `$BS`/topic from a session that then became inconsistent. Fix: make sure `bootstrap.sh` has no `set -euo pipefail` (see the warning above), re-run `source bootstrap.sh` in both tabs, and reproduce from scratch. |
