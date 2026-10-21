#!/usr/bin/env bash
# check-setup.sh — verifies the host is ready for the labs.
#
# Checks, in order, and shows a ✅/❌ checklist:
#   1. Java installed (11+).
#   2. aws-msk-iam-auth-*.jar present in the classpath/libs.
#   3. $BS defined and client.properties present.
#   4. Connection to the cluster (kafka-topics --list responds).
#   5. The lab topics (orders, customers, orders-avro) exist.
#   6. Permissions: create and delete a test topic.
#
# Usage:
#   ./check-setup.sh [--kafka-home /home/ec2-user/kafka] [--client-properties path]
#
# Exit code: 0 if everything passes, 1 if something fails.

set -uo pipefail

KAFKA_HOME="${KAFKA_HOME:-/home/ec2-user/kafka}"
CLIENT_PROPERTIES="${CLIENT_PROPERTIES:-$KAFKA_HOME/config/client-iam.properties}"
PROBE_TOPIC="check-setup-probe-$$"
FAILED=0

ok()   { echo "  ✅ $1"; }
fail() { echo "  ❌ $1"; FAILED=1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --kafka-home) KAFKA_HOME="$2"; shift 2 ;;
    --client-properties) CLIENT_PROPERTIES="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [--kafka-home PATH] [--client-properties PATH]"
      exit 0
      ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

echo "== check-setup.sh =="

# 1. Java (the pre-deployed workshop host ships Corretto 11; the IAM auth
# JAR works fine with 11+, so the real minimum accepted here is 11, not 17).
if command -v java >/dev/null 2>&1; then
  JAVA_VER="$(java -version 2>&1 | head -n1 | grep -oE '"[0-9]+' | tr -d '"')"
  if [[ -n "$JAVA_VER" && "$JAVA_VER" -ge 11 ]]; then
    ok "Java $JAVA_VER detected (>= 11)"
  else
    fail "Java detected but version < 11 (found: ${JAVA_VER:-unknown})"
  fi
else
  fail "Java not found in PATH"
fi

# 2. IAM auth JAR — check $CLASSPATH first (bootstrap.sh exports it), and if
# not, look under $KAFKA_HOME following symlinks (-L, needed because
# /home/ec2-user/kafka is a symlink to kafka351/).
JAR_FOUND=""
if [[ -n "${CLASSPATH:-}" && -f "${CLASSPATH:-}" ]]; then
  JAR_FOUND="$CLASSPATH"
else
  JAR_FOUND="$(find -L "$KAFKA_HOME" -iname 'aws-msk-iam-auth*.jar' 2>/dev/null | head -n1)"
fi
if [[ -n "$JAR_FOUND" ]]; then
  ok "aws-msk-iam-auth JAR found: $JAR_FOUND"
else
  fail "Could not find aws-msk-iam-auth-*.jar under $KAFKA_HOME or in \$CLASSPATH"
fi

# 3. $BS and client.properties
if [[ -n "${BS:-}" ]]; then
  ok "\$BS is set"
else
  fail "\$BS is not set (run: source bootstrap.sh)"
fi

if [[ -f "$CLIENT_PROPERTIES" ]]; then
  ok "client.properties found: $CLIENT_PROPERTIES"
else
  fail "Could not find $CLIENT_PROPERTIES (check --client-properties or run bootstrap.sh)"
fi

# 4. Connection to the cluster
if [[ -n "${BS:-}" && -f "$CLIENT_PROPERTIES" && -x "$KAFKA_HOME/bin/kafka-topics.sh" ]]; then
  if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BS" \
      --command-config "$CLIENT_PROPERTIES" --list >/tmp/check-setup-list.$$ 2>&1; then
    ok "Connection to the cluster OK (kafka-topics --list responds)"
  else
    fail "Could not list topics. Details:"
    sed 's/^/      /' /tmp/check-setup-list.$$
  fi
  rm -f /tmp/check-setup-list.$$
else
  fail "Skipping connection test (prerequisites not met)"
fi

# 5. The lab topics actually exist (orders/customers/orders-avro are
# supposed to be pre-created, but bootstrap.sh also creates them
# idempotently as a safety net — this check just confirms they're there
# before the attendee hits a "Topic does not exist" error mid-lab).
if [[ -n "${BS:-}" && -f "$CLIENT_PROPERTIES" && -x "$KAFKA_HOME/bin/kafka-topics.sh" ]]; then
  EXISTING="$("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BS" \
      --command-config "$CLIENT_PROPERTIES" --list 2>/dev/null)"
  for t in orders customers orders-avro; do
    if echo "$EXISTING" | grep -qx "$t"; then
      ok "Topic '$t' exists"
    else
      fail "Topic '$t' is missing — run: source bootstrap.sh (it creates missing lab topics too)"
    fi
  done
else
  fail "Skipping lab-topics check (prerequisites not met)"
fi

# 6. Permissions: create and delete a test topic
if [[ -n "${BS:-}" && -f "$CLIENT_PROPERTIES" && -x "$KAFKA_HOME/bin/kafka-topics.sh" ]]; then
  if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BS" \
      --command-config "$CLIENT_PROPERTIES" \
      --create --topic "$PROBE_TOPIC" --partitions 1 --replication-factor 2 >/tmp/check-setup-create.$$ 2>&1; then
    ok "Topic creation permission OK (test topic: $PROBE_TOPIC)"
    if "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$BS" \
        --command-config "$CLIENT_PROPERTIES" \
        --delete --topic "$PROBE_TOPIC" >/tmp/check-setup-delete.$$ 2>&1; then
      ok "Topic deletion permission OK (cleanup done)"
    else
      fail "Could not delete test topic $PROBE_TOPIC. Delete it manually. Details:"
      sed 's/^/      /' /tmp/check-setup-delete.$$
    fi
  else
    fail "Could not create the test topic. Details:"
    sed 's/^/      /' /tmp/check-setup-create.$$
  fi
  rm -f /tmp/check-setup-create.$$ /tmp/check-setup-delete.$$
else
  fail "Skipping permissions test (prerequisites not met)"
fi

echo "===================="
if [[ "$FAILED" -eq 0 ]]; then
  echo "Result: ALL GOOD ✅"
  exit 0
else
  echo "Result: SOME ISSUES ❌ — review the items flagged above"
  exit 1
fi
