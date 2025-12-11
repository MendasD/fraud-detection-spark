#!/bin/bash
# wait-for-zookeeper.sh

set -e

echo "⏳ Attente de Zookeeper..."

# Retry jusqu'à 60 secondes
RETRY_COUNT=0
MAX_RETRIES=30

until nc -z zookeeper 2181 || [ $RETRY_COUNT -eq $MAX_RETRIES ]; do
  echo "⏳ Zookeeper pas encore prêt... tentative $((RETRY_COUNT+1))/$MAX_RETRIES"
  RETRY_COUNT=$((RETRY_COUNT+1))
  sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
  echo "❌ Timeout : Zookeeper n'est pas accessible après $((MAX_RETRIES*2)) secondes"
  exit 1
fi

echo "✅ Zookeeper est prêt !"

# Démarrer Kafka
exec /etc/confluent/docker/run