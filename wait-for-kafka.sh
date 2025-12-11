#!/bin/bash
set -e

echo "================================="
echo "Attente de Kafka..."
echo "================================="

KAFKA_HOST=${KAFKA_BOOTSTRAP_SERVERS%:*}
KAFKA_PORT=${KAFKA_BOOTSTRAP_SERVERS#*:}

echo "Host: $KAFKA_HOST"
echo "Port: $KAFKA_PORT"

RETRY_COUNT=0
MAX_RETRIES=60

until (echo > /dev/tcp/$KAFKA_HOST/$KAFKA_PORT) 2>/dev/null || [ $RETRY_COUNT -eq $MAX_RETRIES ]; do
  RETRY_COUNT=$((RETRY_COUNT+1))
  echo "⏳ Tentative $RETRY_COUNT/$MAX_RETRIES..."
  sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
  echo "❌ ERREUR: Kafka non accessible après 120 secondes"
  exit 1
fi

echo "================================="
echo "✅ Kafka est prêt!"
echo "⏳ Attente de 10s supplémentaires pour l'initialisation..."
echo "================================="
sleep 10

echo "🚀 Démarrage de l'application..."
exec "$@"