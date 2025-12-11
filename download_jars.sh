#!/bin/bash
# Script pour télécharger les JARs Spark nécessaires

JARS_DIR="/app/jars"
mkdir -p $JARS_DIR

echo "📦 Téléchargement des JARs Spark..."

# Spark SQL Kafka
wget -q -O $JARS_DIR/spark-sql-kafka-0-10_2.12-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

# Kafka clients
wget -q -O $JARS_DIR/kafka-clients-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar

# Commons Pool2 (dépendance)
wget -q -O $JARS_DIR/commons-pool2-2.11.1.jar \
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Spark Token Provider Kafka
wget -q -O $JARS_DIR/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

echo "✅ JARs téléchargés!"