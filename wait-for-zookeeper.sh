#!/bin/bash
# wait-for-zookeeper.sh

set -e

echo "⏳ Attente de Zookeeper sur ${KAFKA_ZOOKEEPER_CONNECT}..."

# Extraire host et port
ZOOKEEPER_HOST=$(echo $KAFKA_ZOOKEEPER_CONNECT | cut -d: -f1)
ZOOKEEPER_PORT=$(echo $KAFKA_ZOOKEEPER_CONNECT | cut -d: -f2)

RETRY_COUNT=0
MAX_RETRIES=60  # 2 minutes max

# Utiliser nc (netcat) pour vérifier la connexion
until nc -z $ZOOKEEPER_HOST $ZOOKEEPER_PORT 2>/dev/null || [ $RETRY_COUNT -eq $MAX_RETRIES ]; do
  echo "⏳ Tentative $((RETRY_COUNT+1))/$MAX_RETRIES - Zookeeper pas encore prêt..."
  RETRY_COUNT=$((RETRY_COUNT+1))
  sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
  echo "❌ ERREUR: Zookeeper non accessible après $((MAX_RETRIES*2)) secondes"
  echo "❌ Impossible de se connecter à $ZOOKEEPER_HOST:$ZOOKEEPER_PORT"
  exit 1
fi

echo "✅ Zookeeper est disponible sur $ZOOKEEPER_HOST:$ZOOKEEPER_PORT"
echo "🚀 Démarrage de Kafka..."

# Démarrer Kafka avec la commande originale
exec /etc/confluent/docker/run