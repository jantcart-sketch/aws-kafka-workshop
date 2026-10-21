#!/usr/bin/env bash
# cleanup-topics.sh — deletes the lab's topics (orders, customers,
# orders-avro, and optionally orders-live if the firehose was used in this
# account) when you're done. Defaults to --dry-run; you must pass
# --execute explicitly to actually delete anything.
#
# Workshop Studio model: each account belongs to a single attendee, so
# there are no <username>-suffixed names to look for — topics are simply
# called "orders", "customers", "orders-avro".
#
# Usage:
#   ./cleanup-topics.sh [--kafka-home /home/ec2-user/kafka] [--client-properties path]
#                        [--include-live] [--execute]

set -euo pipefail

KAFKA_HOME="${KAFKA_HOME:-/home/ec2-user/kafka}"
CLIENT_PROPERTIES="${CLIENT_PROPERTIES:-$KAFKA_HOME/config/client-iam.properties}"
EXECUTE=0
INCLUDE_LIVE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --kafka-home) KAFKA_HOME="$2"; shift 2 ;;
    --client-properties) CLIENT_PROPERTIES="$2"; shift 2 ;;
    --execute) EXECUTE=1; shift ;;
    --include-live) INCLUDE_LIVE=1; shift ;;
    -h|--help)
      echo "Usage: $0 [--kafka-home PATH] [--client-properties PATH] [--include-live] [--execute]"
      echo "Without --execute, this only does a dry-run (lists what it would delete without deleting anything)."
      exit 0
      ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "${BS:-}" ]]; then
  echo "ERROR: \$BS is not set. Run: source bootstrap.sh" >&2
  exit 1
fi
if [[ ! -x "$KAFKA_HOME/bin/kafka-topics.sh" ]]; then
  echo "ERROR: could not find $KAFKA_HOME/bin/kafka-topics.sh" >&2
  exit 1
fi

echo "==> Listing cluster topics..."
ALL_TOPICS="$("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BS" \
  --command-config "$CLIENT_PROPERTIES" --list)"

MATCHED="$(echo "$ALL_TOPICS" | grep -E '^(orders|customers)$|^orders-avro$' || true)"

if [[ "$INCLUDE_LIVE" -eq 1 ]]; then
  LIVE="$(echo "$ALL_TOPICS" | grep -E '^orders-live$' || true)"
  MATCHED="$(printf '%s\n%s\n' "$MATCHED" "$LIVE" | grep -v '^$' || true)"
fi

if [[ -z "$MATCHED" ]]; then
  echo "==> No matching lab topics found. Nothing to do."
  exit 0
fi

COUNT="$(echo "$MATCHED" | wc -l | tr -d ' ')"
echo "==> Matching topics: $COUNT"
echo "$MATCHED" | sed 's/^/    - /'

if [[ "$EXECUTE" -eq 0 ]]; then
  echo
  echo "==> DRY-RUN: nothing deleted. Pass --execute to actually delete."
  exit 0
fi

echo
echo "==> Deleting $COUNT topics (--execute)..."
FAILED=0
while IFS= read -r topic; do
  [[ -z "$topic" ]] && continue
  if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BS" \
      --command-config "$CLIENT_PROPERTIES" \
      --delete --topic "$topic" >/dev/null 2>&1; then
    echo "    deleted: $topic"
  else
    echo "    ERROR deleting: $topic" >&2
    FAILED=1
  fi
done <<< "$MATCHED"

if [[ "$FAILED" -eq 0 ]]; then
  echo "==> Cleanup completed with no errors."
else
  echo "==> Cleanup completed WITH errors on some topics (see above)." >&2
  exit 1
fi
