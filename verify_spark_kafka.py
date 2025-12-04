"""
Script de vérification de l'installation Spark + Kafka
Exécuter avant de lancer le détecteur de fraudes
"""

import sys
import subprocess

def check_spark_version():
    """Vérifie la version de Spark installée"""
    print("🔍 Vérification de Spark...")
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("test").getOrCreate()
        print(f"✅ Spark version: {spark.version}")
        
        # Vérifier Scala
        scala_version = spark.sparkContext._jvm.scala.util.Properties.versionString()
        print(f"✅ Scala version: {scala_version}")
        
        spark.stop()
        return True
    except Exception as e:
        print(f"❌ Erreur Spark: {e}")
        return False


def check_kafka_connection():
    """Vérifie la connexion à Kafka"""
    print("\n🔍 Vérification de Kafka...")
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            consumer_timeout_ms=5000
        )
        topics = consumer.topics()
        print(f"✅ Kafka accessible - Topics: {topics}")
        consumer.close()
        return True
    except Exception as e:
        print(f"❌ Erreur Kafka: {e}")
        return False


def check_spark_packages():
    """Vérifie si les packages Spark sont installés"""
    print("\n🔍 Vérification des packages Spark...")
    try:
        from pyspark.sql import SparkSession
        
        # Essayer de créer une session avec les packages Kafka
        spark = SparkSession.builder \
            .appName("test_kafka") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
            .master("local[1]") \
            .getOrCreate()
        
        print("✅ Package Kafka chargé")
        spark.stop()
        return True
    except Exception as e:
        print(f"❌ Erreur chargement package: {e}")
        return False


def suggest_solutions():
    """Suggère des solutions selon les erreurs détectées"""
    print("\n" + "="*60)
    print("💡 SOLUTIONS POSSIBLES")
    print("="*60)
    
    print("\n1️⃣ TÉLÉCHARGER MANUELLEMENT LES JARS")
    print("   Téléchargez ces fichiers JAR :")
    print("   - spark-sql-kafka-0-10_2.12-3.5.3.jar")
    print("   - kafka-clients-3.5.1.jar")
    print("   - spark-token-provider-kafka-0-10_2.12-3.5.3.jar")
    print("   - commons-pool2-2.11.1.jar")
    print("\n   URLs Maven Repository:")
    print("   https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.12/3.5.3")
    
    print("\n2️⃣ PLACER LES JARS DANS LE DOSSIER JARS")
    print("   Créer: ./jars/")
    print("   Et modifier spark_utils.py pour ajouter:")
    print('   .config("spark.jars", "./jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,./jars/kafka-clients-3.5.1.jar")')
    
    print("\n3️⃣ UTILISER SPARK-SUBMIT")
    print("   Au lieu de python ml_fraud_detector.py, utiliser:")
    print("   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 ml_fraud_detector.py")
    
    print("\n4️⃣ VÉRIFIER LA VERSION SCALA")
    print("   Votre Spark utilise peut-être Scala 2.13, dans ce cas:")
    print("   Remplacer _2.12 par _2.13 dans les packages")
    
    print("\n5️⃣ RÉINSTALLER PYSPARK")
    print("   pip uninstall pyspark")
    print("   pip install pyspark==3.5.3")


if __name__ == "__main__":
    print("=" * 60)
    print("🔧 VÉRIFICATION CONFIGURATION SPARK + KAFKA")
    print("=" * 60)
    
    results = []
    results.append(("Spark", check_spark_version()))
    results.append(("Kafka", check_kafka_connection()))
    results.append(("Packages", check_spark_packages()))
    
    print("\n" + "=" * 60)
    print("📊 RÉSUMÉ")
    print("=" * 60)
    
    all_ok = True
    for name, status in results:
        icon = "✅" if status else "❌"
        print(f"{icon} {name}: {'OK' if status else 'ERREUR'}")
        if not status:
            all_ok = False
    
    if not all_ok:
        suggest_solutions()
    else:
        print("\n🎉 Tout est OK ! Vous pouvez lancer le détecteur de fraudes.")