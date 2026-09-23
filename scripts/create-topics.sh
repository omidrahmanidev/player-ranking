#!/usr/bin/env bash
set -euo pipefail
bootstrap=${BOOTSTRAP_SERVERS:-localhost:9092}
partitions=${PARTITIONS:-32}
replication=${REPLICATION_FACTOR:-3}
min_isr=${MIN_ISR:-2}
topics_bin=${KAFKA_TOPICS_BIN:-/opt/kafka/bin/kafka-topics.sh}
create_topic() {
  "$topics_bin" --bootstrap-server "$bootstrap" --create --if-not-exists \
    --topic "$1" --partitions "$partitions" --replication-factor "$replication" \
    --config "min.insync.replicas=$min_isr" --config "cleanup.policy=$2" \
    --config "retention.ms=$3"
}
create_topic score-events-v1 delete 86400000
create_topic ranking-totals-v1 compact -1
create_topic ranking-audit-v1 delete 604800000
