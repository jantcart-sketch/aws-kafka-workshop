#!/usr/bin/env bash
# bootstrap.sh — prepares the workshop host environment for the labs.
#
# Workshop Studio model: each attendee has their OWN isolated AWS account,
# with their own MSK cluster and their own client host (deployed by the same
# CloudFormation stack in every account). There's nothing to share or
# isolate per user — that's why this script needs no --precreate or
# --users-file: it only sets up THIS cluster, for the attendee sitting in
# front of THIS host.
#
# What it does:
#   1. Discovers this account's MSK cluster by name (every account deploys
#      the same stack, so the name is always the same; the ARN differs per
#      account, which is why we don't hardcode it).
#   2. Resolves its IAM bootstrap string.
#   3. Generates client-iam.properties for the Kafka CLI.
#   4. Exports $BS and $CLASSPATH into the current shell (use with `source`).
#
# Usage (on the host, as ec2-user):
#   source bootstrap.sh [--cluster-name MSKCluster-msk-labs-default] \
#       [--region us-east-2] [--kafka-home /home/ec2-user/kafka]
#
# IMPORTANT: this script is meant to be `source`d in an interactive
# session, so it does NOT use `set -euo pipefail`. If it did, those
# options (-e: kill the shell on the first error, -u: error on using an
# undefined variable) would stay active in your terminal forever after
# loading it — not just while the script runs — and any normal command
# that fails, or any unexported variable (common in an interactive
# session), could kill your session outright. The script instead uses
# explicit checks (if/return) at each critical step.

DEFAULT_CLUSTER_NAME="MSKCluster-msk-labs-default"
DEFAULT_IAM_JAR="/home/ec2-user/kafka351/aws-msk-iam-auth-2.3.2-all.jar"

CLUSTER_NAME="${DEFAULT_CLUSTER_NAME}"
REGION="${AWS_REGION:-us-east-2}"
KAFKA_HOME="${KAFKA_HOME:-/home/ec2-user/kafka}"
CLIENT_PROPERTIES="${CLIENT_PROPERTIES:-$KAFKA_HOME/config/client-iam.properties}"

usage() {
  cat <<EOF
Usage: source $0 [options]

Options:
  --cluster-name <name>   Name of the MSK cluster in this account (default:
                            $DEFAULT_CLUSTER_NAME, the Express cluster the
                            workshop stack deploys in every account)
  --region <region>        AWS region (default: \$AWS_REGION or us-east-2)
  --kafka-home <path>      Kafka CLI install path (default: /home/ec2-user/kafka)
  -h, --help               Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --cluster-name) CLUSTER_NAME="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --kafka-home) KAFKA_HOME="$2"; shift 2 ;;
    -h|--help) usage; return 0 2>/dev/null || exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; return 1 2>/dev/null || exit 1 ;;
  esac
done

echo "==> Looking for cluster '$CLUSTER_NAME' in this account ($REGION)..."
CLUSTER_ARN="$(aws kafka list-clusters-v2 \
  --region "$REGION" \
  --cluster-name-filter "$CLUSTER_NAME" \
  --query 'ClusterInfoList[0].ClusterArn' \
  --output text 2>/dev/null || true)"

if [[ -z "$CLUSTER_ARN" || "$CLUSTER_ARN" == "None" ]]; then
  echo "ERROR: no cluster named '$CLUSTER_NAME' found in this account/region." >&2
  echo "       Check with: aws kafka list-clusters-v2 --region $REGION" >&2
  return 1 2>/dev/null || exit 1
fi
echo "    found: $CLUSTER_ARN"

echo "==> Resolving bootstrap brokers (IAM)..."
BS="$(aws kafka get-bootstrap-brokers \
  --region "$REGION" \
  --cluster-arn "$CLUSTER_ARN" \
  --query 'BootstrapBrokerStringSaslIam' \
  --output text)"

if [[ -z "$BS" || "$BS" == "None" ]]; then
  echo "ERROR: could not get BootstrapBrokerStringSaslIam. Is IAM auth enabled on the cluster?" >&2
  return 1 2>/dev/null || exit 1
fi

export BS
echo "==> \$BS exported: $BS"

export CLASSPATH="$DEFAULT_IAM_JAR"
echo "==> \$CLASSPATH exported: $CLASSPATH"
if [[ ! -f "$CLASSPATH" ]]; then
  echo "    WARNING: JAR not found at that path, check with 'find / -iname aws-msk-iam-auth*.jar'" >&2
fi

mkdir -p "$(dirname "$CLIENT_PROPERTIES")"
echo "==> Generating $CLIENT_PROPERTIES (AWS_MSK_IAM auth for the Kafka CLI)..."
cat > "$CLIENT_PROPERTIES" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF
echo "    OK: $CLIENT_PROPERTIES"

# Ensure the lab topics exist. They're meant to be pre-created before the
# workshop, but that step can be skipped or fail silently per-account — so
# bootstrap.sh creates them here too, idempotently (--if-not-exists), as a
# safety net every time an attendee connects. NOTE: on Express brokers,
# MSK ignores whatever --replication-factor you pass and always creates
# the topic with RF=3 (and min.insync.replicas=2) — see AWS's own docs,
# "Five operational differences to check before adopting MSK Express
# brokers". We pass 2 below only to match what the labs/docs show; the
# actual topic will come back as RF=3 either way, which is expected.
echo "==> Ensuring lab topics exist (orders, customers, orders-avro)..."
TOPICS_SH="$KAFKA_HOME/bin/kafka-topics.sh"
if [[ -x "$TOPICS_SH" ]]; then
  "$TOPICS_SH" --bootstrap-server "$BS" --command-config "$CLIENT_PROPERTIES" \
    --create --if-not-exists --topic orders --partitions 3 --replication-factor 2 >/dev/null 2>&1
  "$TOPICS_SH" --bootstrap-server "$BS" --command-config "$CLIENT_PROPERTIES" \
    --create --if-not-exists --topic customers --partitions 1 --replication-factor 2 \
    --config cleanup.policy=compact >/dev/null 2>&1
  "$TOPICS_SH" --bootstrap-server "$BS" --command-config "$CLIENT_PROPERTIES" \
    --create --if-not-exists --topic orders-avro --partitions 1 --replication-factor 2 >/dev/null 2>&1
  echo "    OK: orders, customers, orders-avro present (created if missing)"
else
  echo "    WARNING: $TOPICS_SH not found/executable, skipped topic creation" >&2
fi

echo "==> Done. Test with:"
echo "    $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server \$BS --command-config $CLIENT_PROPERTIES --list"
