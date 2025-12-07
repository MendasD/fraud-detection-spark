"""
Utilitaires pour créer et configurer les sessions Spark.
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from dotenv import load_dotenv
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.utils.logger import setup_logger

load_dotenv()
logger = setup_logger(__name__)

# Spécifier le chemin de l'executable pour python
# chemin python de l'environnement virtuel
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = os.getenv('PYSPARK_PYTHON', python_path)
os.environ["PYSPARK_DRIVER_PYTHON"] = os.getenv('PYSPARK_PYTHON', python_path)
os.environ["JAVA_HOME"] = os.getenv('JAVA_HOME')
#os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-21.0.6.7-hotspot" # Java 17 compatible pour spark 3.5.x

# Configurer HADOOP_HOME pour Windows si défini dans .env
hadoop_home = os.getenv('HADOOP_HOME')
if hadoop_home and os.path.exists(hadoop_home):
    os.environ['HADOOP_HOME'] = hadoop_home
    bin_path = os.path.join(hadoop_home, 'bin')
    if bin_path not in os.environ.get('PATH', ''):
        os.environ['PATH'] = f"{bin_path};{os.environ.get('PATH', '')}"
    logger.info(f"HADOOP_HOME configuré: {hadoop_home}")
    

def create_spark_session(
    app_name: str = None,
    master: str = None,
    log_level: str = None
) -> SparkSession:
    """
    Crée une session Spark configurée pour le streaming Kafka.
    
    Args:
        app_name: Nom de l'application Spark
        master: URL du master Spark (ex: 'local[*]')
        log_level: Niveau de log ('INFO', 'WARN', 'ERROR')
        
    Returns:
        SparkSession configurée
    """
    app_name = app_name or os.getenv('SPARK_APP_NAME', 'FraudDetectionSystem')
    master = master or os.getenv('SPARK_MASTER', 'local[*]')
    log_level = log_level or os.getenv('SPARK_LOG_LEVEL', 'WARN')
    
    logger.info(f"Création de la session Spark: {app_name}")
    logger.info(f"Master: {master}")
    
    try:
        # Configuration Spark
        conf = SparkConf()
        conf.setAppName(app_name)
        conf.setMaster(master)
        
        # Optimisations pour le streaming
        conf.set("spark.streaming.stopGracefullyOnShutdown", "true") # arrêter le streaming proprement
        conf.set("spark.sql.streaming.schemaInference", "true") # détecter le schéma des données
        conf.set("spark.sql.shuffle.partitions", "6")  # Correspond aux partitions Kafka
        
        # Configuration mémoire
        conf.set("spark.driver.memory", "2g")
        conf.set("spark.executor.memory", "2g")
        
        # Configuration pour Kafka
        conf.set(
            "spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.7"
        )

        
        # Créer la session
        spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        
        # Définir le niveau de log
        spark.sparkContext.setLogLevel(log_level)
        
        logger.info(f".......... Session Spark créée - Version: {spark.version} ..................")
        
        return spark
        
    except Exception as e:
        logger.error(f" Erreur lors de la création de la session Spark: {e}")
        raise


def read_kafka_stream(
    spark: SparkSession,
    kafka_bootstrap_servers: str = None,
    topic: str = None,
    starting_offsets: str = "latest"
) -> DataFrame:
    """
    Lit un stream Kafka et retourne un DataFrame Spark Streaming.
    
    Args:
        spark: Session Spark
        kafka_bootstrap_servers: Adresse du broker Kafka
        topic: Nom du topic à lire
        starting_offsets: 'earliest' ou 'latest'
        
    Returns:
        DataFrame Spark Streaming
    """
    servers = kafka_bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic_name = topic or os.getenv('KAFKA_TOPIC', 'transactions')
    
    logger.info(f"Lecture du stream Kafka - Topic: {topic_name}")
    logger.info(f"Bootstrap servers: {servers}")
    
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", servers) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", starting_offsets) \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 10000) \
            .load()
        
        logger.info(" Stream Kafka connecté")
        
        return df
        
    except Exception as e:
        logger.error(f" Erreur lors de la lecture du stream Kafka: {e}")
        raise


def parse_kafka_message(df: DataFrame) -> DataFrame:
    """
    Parse les messages Kafka (JSON) en colonnes structurées.
    
    Args:
        df: DataFrame Spark Streaming depuis Kafka
        
    Returns:
        DataFrame avec colonnes parsées
    """
    from pyspark.sql.functions import from_json, col
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType, TimestampType
    
    logger.info("Parsing des messages Kafka")
    
    # Définir le schéma des transactions
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("location_lat", DoubleType(), False),
        StructField("location_lon", DoubleType(), False),
        StructField("device_id", StringType(), False),
        StructField("is_online", BooleanType(), False),
        StructField("is_fraud", IntegerType(), False),
        StructField("card_last_4", StringType(), True),
        StructField("cvv_provided", BooleanType(), True),
    ])
    
    # Parser le JSON depuis la colonne 'value' de Kafka
    df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Convertir timestamp en TimestampType
    df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    
    logger.info("***** Messages parsés *****")
    
    return df


def write_to_console(df: DataFrame, output_mode: str = "append", trigger: str = "5 seconds"):
    """
    Écrit le stream dans la console (pour debug).
    
    Args:
        df: DataFrame à écrire
        output_mode: 'append', 'complete', 'update'
        trigger: Intervalle de déclenchement
        
    Returns:
        StreamingQuery
    """
    logger.info("Démarrage du stream vers la console")
    
    query = df.writeStream \
        .outputMode(output_mode) \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime=trigger) \
        .start()
    
    return query


def write_to_parquet(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    output_mode: str = "append",
    trigger: str = "10 seconds"
):
    """
    Écrit le stream dans des fichiers Parquet (pour persistence).
    
    Args:
        df: DataFrame à écrire
        output_path: Chemin de sortie des fichiers Parquet
        checkpoint_path: Chemin pour les checkpoints Spark
        output_mode: 'append', 'complete', 'update'
        trigger: Intervalle de déclenchement
        
    Returns:
        StreamingQuery
    """
    logger.info(f"Démarrage du stream vers Parquet: {output_path}")
    
    query = df.writeStream \
        .outputMode(output_mode) \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime=trigger) \
        .start()
    
    return query


def write_to_memory(
    df: DataFrame,
    table_name: str = "fraud_detection",
    output_mode: str = "append",
    trigger: str = "5 seconds"
):
    """
    Écrit le stream dans une table en mémoire (pour dashboard).
    
    Args:
        df: DataFrame à écrire
        table_name: Nom de la table en mémoire
        output_mode: 'append', 'complete', 'update'
        trigger: Intervalle de déclenchement
        
    Returns:
        StreamingQuery
    """
    logger.info(f"Démarrage du stream vers la table mémoire: {table_name}")
    
    query = df.writeStream \
        .outputMode(output_mode) \
        .format("memory") \
        .queryName(table_name) \
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
