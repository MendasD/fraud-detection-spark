#!/bin/bash
set -e

echo "================================="
echo "Attente de Zookeeper..."
echo "================================="

# Extraire host et port
ZOOKEEPER_HOST=${KAFKA_ZOOKEEPER_CONNECT%:*}
ZOOKEEPER_PORT=${KAFKA_ZOOKEEPER_CONNECT#*:}

echo "Host: $ZOOKEEPER_HOST"
echo "Port: $ZOOKEEPER_PORT"

RETRY_COUNT=0
MAX_RETRIES=60

# Utiliser /dev/tcp (natif bash - pas besoin de netcat)
until (echo > /dev/tcp/$ZOOKEEPER_HOST/$ZOOKEEPER_PORT) 2>/dev/null || [ $RETRY_COUNT -eq $MAX_RETRIES ]; do
  RETRY_COUNT=$((RETRY_COUNT+1))
  echo "⏳ Tentative $RETRY_COUNT/$MAX_RETRIES..."
  sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
  echo "❌ ERREUR: Zookeeper non accessible après 120 secondes"
  exit 1
fi

echo "================================="
echo "✅ Zookeeper est prêt!"
echo "🚀 Démarrage de Kafka..."
echo "================================="

# Démarrer Kafka
exec /etc/confluent/docker/run