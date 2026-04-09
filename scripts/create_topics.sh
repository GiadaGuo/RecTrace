#!/bin/bash
# Create all required Kafka topics
# Usage: bash scripts/create_topics.sh [kafka-bootstrap-server]

BOOTSTRAP=${1:-"localhost:9092"}
KAFKA_CONTAINER="kafka"

echo "Creating Kafka topics on $BOOTSTRAP ..."

TOPICS=(
  "ods_user_behavior"
  "dwd_behavior_with_dim"
  "dws_user_item_feature"
  "dws_user_feature"
  "dws_item_feature"
)

for TOPIC in "${TOPICS[@]}"; do
  docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions 3 \
    --replication-factor 1
  echo "  Created topic: $TOPIC"
done

echo ""
echo "All topics:"
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
