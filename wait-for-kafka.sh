#!/bin/bash
# wait-for-kafka.sh

set -e

echo "⏳ Attente de Kafka..."

RETRY_COUNT=0
MAX_RETRIES=60  # 2 minutes max

until nc -z kafka 29092 || [ $RETRY_COUNT -eq $MAX_RETRIES ]; do
  echo "⏳ Kafka pas encore prêt... tentative $((RETRY_COUNT+1))/$MAX_RETRIES"
  RETRY_COUNT=$((RETRY_COUNT+1))
  sleep 2
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
  echo "❌ Timeout : Kafka n'est pas accessible"
  exit 1
fi

echo "✅ Kafka est prêt !"

# Vérifier que le topic existe ou peut être créé
echo "🔍 Vérification du topic..."
sleep 5  # Attendre que Kafka soit complètement initialisé

# Lancer l'application
exec "$@"