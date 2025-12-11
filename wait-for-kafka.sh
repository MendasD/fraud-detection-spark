#!/bin/bash
# wait-for-kafka.sh

set -e

KAFKA_HOST=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d: -f1)
KAFKA_PORT=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d: -f2)

echo "⏳ Attente de Kafka sur $KAFKA_HOST:$KAFKA_PORT..."

RETRY_COUNT=0
MAX_RETRIES=60

until nc -z $KAFKA_HOST $KAFKA_PORT 2>/dev/null || [ $RETRY_COUNT -eq $MAX_RETRIES ]; do
  echo "⏳ Tentative $((RETRY_COUNT+1))/$MAX_RETRIES..."
  RETRY_COUNT=$((RETRY_COUNT+1))
  sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
  echo "❌ ERREUR: Kafka non accessible après $((MAX_RETRIES*2)) secondes"
  exit 1
fi

echo "✅ Kafka est disponible !"
echo "⏳ Attente supplémentaire de 10s pour l'initialisation complète..."
sleep 10

echo "🚀 Démarrage de l'application..."
exec "$@"