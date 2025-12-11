"""
Utilitaires Spark avec configuration corrigée pour Kafka
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
from dotenv import load_dotenv
from src.utils.logger import setup_logger
import time

load_dotenv()
logger = setup_logger(__name__)

# Spécifier le chemin de l'executable pour python
# chemin python de l'environnement virtuel
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = os.getenv('PYSPARK_PYTHON', python_path)
os.environ["PYSPARK_DRIVER_PYTHON"] = os.getenv('PYSPARK_PYTHON', python_path)
java_home = os.getenv('JAVA_HOME') # Java 21 compatible pour spark 3.5.x
if java_home:
    os.environ["JAVA_HOME"] = java_home
else:
    pass # on laisse java du conteneur

# Configurer HADOOP_HOME pour Windows si défini dans .env
hadoop_home = os.getenv('HADOOP_HOME')
if hadoop_home and os.path.exists(hadoop_home):
    os.environ['HADOOP_HOME'] = hadoop_home
    bin_path = os.path.join(hadoop_home, 'bin')
    if bin_path not in os.environ.get('PATH', ''):
        os.environ['PATH'] = f"{bin_path};{os.environ.get('PATH', '')}"
    logger.info(f"HADOOP_HOME configuré: {hadoop_home}")

def create_spark_session(app_name: str = "FraudDetectionML"):
    """
    Crée une session Spark avec la configuration correcte pour Kafka.
    
    IMPORTANT: Utilise spark.jars.packages pour télécharger automatiquement
    les dépendances nécessaires.
    """
    # Détecter la version de Scala (généralement 2.12 ou 2.13)
    spark_version = "3.5.3"  # Votre version Spark
    scala_version = "2.12"   # Version Scala (ajuster si nécessaire)

    # Configuration Ivy pour eviter les problemes de permission sur Railway
    ivy_home = os.environ.get('IVY_HOME', '/tmp/.ivy2')
    
    # Packages nécessaires
    packages = [
        f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
        f"org.apache.kafka:kafka-clients:3.5.1"
    ]

    jars_path = "/app/jars"
    jars = [
        f"{jars_path}/spark-sql-kafka-0-10_2.12-3.5.0.jar",
        f"{jars_path}/kafka-clients-3.5.0.jar",
        f"{jars_path}/commons-pool2-2.11.1.jar",
        f"{jars_path}/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
    ]

    logger.info(f" Packages Spark: {packages}")
    logger.info(f" Spark Version: {spark_version}")
    logger.info(f" Scala Version: {scala_version}")
    logger.info(f"Création de la session spark")

    # Chekpoint indicé par le temps
    checkpoint_path = f"./data/checkpoints/fraud_detection_ml_{int(time.time())}"
    
    # .config("spark.jars.packages", ",".join(packages)) \
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.jars.ivy", ivy_home) \
        .config("spark.local.dir", "/tmp/spark") \
        .config("spark.sql.streaming.checkpointLocation", f"{checkpoint_path}") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.python.worker.timeout", "300") \
        .config("spark.python.worker.reuse", "true") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "10s") \
        .config("spark.python.profile", "false") \
        .config("spark.executor.defaultJavaOpts", "-Dspark.kafka.allowNonInterruptibleThread=true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN") 
    spark.conf.set("spark.sql.streaming.uninterruptible", "true")
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"=\"=\" Session Spark créée - Version: {spark.version} =\"=\"")   
    return spark


def get_transaction_schema():
    """
    Schéma des transactions pour le parsing JSON.
    """
    logger.info("Création du schéma des transactions")
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("timestamp", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("location_lat", DoubleType(), False),
        StructField("location_lon", DoubleType(), False),
        StructField("is_online", BooleanType(), False),
        StructField("is_fraud", IntegerType(), False),
    ])


def read_kafka_stream(spark: SparkSession):
    """
    Lit le stream Kafka avec la configuration correcte.
    """
    kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    kafka_topic = os.getenv('KAFKA_TOPIC', 'transactions')
    
    logger.info(f"** Connexion à Kafka - Topic: {kafka_topic}, Servers: {kafka_bootstrap_servers}")
    
    try:
        stream = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .load()
        
        logger.info("** Stream Kafka connecté")
        return stream
        
    except Exception as e:
        logger.error(f"* Erreur connexion Kafka: {e}")
        raise


def parse_kafka_message(raw_stream):
    """
    Parse les messages JSON depuis Kafka.
    """
    schema = get_transaction_schema()
    
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")
    
    logger.info("=\"=\" Messages Kafka parsés =\"=\" ")
    
    return parsed_stream


def write_to_console(stream, trigger="5 seconds"):
    """
    Écrit le stream vers la console.
    """
    query = stream \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime=trigger) \
        .option("truncate", False) \
        .option("numRows", 20) \
        .start()
    
    return query


def write_to_memory(stream, table_name="fraud_detection_ml", trigger="5 seconds"):
    """
    Écrit le stream en mémoire pour le dashboard.
    """
    query = stream \
        .writeStream \
        .outputMode("append") \
        .format("memory") \
        .queryName(table_name) \
        .trigger(processingTime=trigger) \
        .start()
    
    return query


def write_to_parquet(stream, output_path, checkpoint_path, trigger="5 seconds"):
    """
    Écrit le stream vers Parquet.
    """
    query = stream \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime=trigger) \
        .start()
    
    return query

if __name__ == "__main__":
    print("*** Test des utilitaires Spark ***\n")
    
    # Créer une session Spark
    print("1. Création de la session Spark...")
    spark = create_spark_session()
    
    print(f"\n Session créée:")
    print(f"  ** Version: {spark.version}")
    print(f"  ** Master: {spark.sparkContext.master}")
    print(f"  ** App Name: {spark.sparkContext.appName}")
    
    # Arrêter la session
    print("\n2. Arrêt de la session...")
    spark.stop()
    
    print("\n ********** Test terminé *************")