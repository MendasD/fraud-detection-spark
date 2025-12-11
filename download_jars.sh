#!/bin/bash
set -e  # Arrêter en cas d'erreur

JARS_DIR="/app/jars"
mkdir -p $JARS_DIR

echo "Téléchargement des JARs Spark..."

# Spark SQL Kafka avec toutes les dépendances
wget -nv -O $JARS_DIR/spark-sql-kafka-0-10_2.12-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

wget -nv -O $JARS_DIR/kafka-clients-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar

wget -nv -O $JARS_DIR/commons-pool2-2.11.1.jar \
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

wget -nv -O $JARS_DIR/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

echo "JARs téléchargés:"
ls -lh $JARS_DIR/