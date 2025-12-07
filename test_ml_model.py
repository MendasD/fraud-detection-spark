"""
Test du modèle ML sans Kafka (avec données simulées)
Utile pour vérifier que le modèle fonctionne avant d'intégrer Kafka
"""

import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, log10, hour, dayofweek, to_timestamp
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from src.utils.logger import setup_logger
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Ajouter le path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

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

def create_simple_spark():
    """Crée une session Spark simple sans Kafka"""
    spark = SparkSession.builder \
        .appName("TestMLModel") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_test_data(spark):
    """Crée des données de test"""
    print("* Création de données de test...")
    
    data = [
        ("TXN001", "USER1", 150.00, "2024-12-03 14:30:00", "MERCH1", "grocery", 48.8566, 2.3522, False, 0),
        ("TXN002", "USER2", 1500.00, "2024-12-03 03:15:00", "MERCH2", "electronics", 48.8566, 2.3522, True, 1),
        ("TXN003", "USER3", 50.00, "2024-12-03 18:45:00", "MERCH3", "restaurant", 48.8566, 2.3522, False, 0),
        ("TXN004", "USER4", 2000.00, "2024-12-03 02:30:00", "MERCH4", "online_shopping", 48.8566, 2.3522, True, 1),
        ("TXN005", "USER5", 75.50, "2024-12-03 12:00:00", "MERCH5", "gas_station", 48.8566, 2.3522, False, 0),
    ]
    
    columns = ["transaction_id", "user_id", "amount", "timestamp", "merchant_id", 
               "merchant_category", "location_lat", "location_lon", "is_online", "is_fraud"]
    
    df = spark.createDataFrame(data, columns)
    print(f"*** {df.count()} transactions créées")
    return df


def create_features(df):
    """Crée les features (même logique que le détecteur)"""
    print("* Création des features...")
    
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    
    # Features temporelles
    df = df.withColumn("hour_of_day", hour(col("timestamp"))) \
           .withColumn("day_of_week", dayofweek(col("timestamp"))) \
           .withColumn("is_weekend", when((dayofweek("timestamp") == 1) | (dayofweek("timestamp") == 7), 1).otherwise(0)) \
           .withColumn("is_unusual_hour", when((hour("timestamp") >= 2) & (hour("timestamp") <= 5), 1).otherwise(0))
    
    # Features de montant
    df = df.withColumn("amount_log", log10(col("amount") + 1)) \
           .withColumn("is_high_amount", when(col("amount") > 500, 1).otherwise(0)) \
           .withColumn("is_round_amount", when(col("amount") % 10 == 0, 1).otherwise(0))
    
    # Features marchand
    df = df.withColumn("is_high_risk_merchant", 
                       when(col("merchant_category").isin(['electronics', 'online_shopping', 'airline', 'hotel']), 1)
                       .otherwise(0))
    
    # Features online/offline
    df = df.withColumn("is_online_int", when(col("is_online") == True, 1).otherwise(0))
    
    print("*** Features créées")
    return df


def apply_model(df, model_path):
    """Applique le modèle ML"""
    print("* Application du modèle...")
    
    feature_columns = [
        'amount', 'amount_log', 'hour_of_day', 'day_of_week',
        'is_weekend', 'is_unusual_hour', 'is_high_amount',
        'is_round_amount', 'is_high_risk_merchant', 'is_online_int',
        'location_lat', 'location_lon'
    ]
    
    try:
        # Charger le modèle
        print(f"* Chargement du modèle: {model_path}")
        model = RandomForestClassificationModel.load(model_path)
        print("*** Modèle chargé")
        
        # Assembler les features
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        df_with_features = assembler.transform(df)
        
        # Prédictions
        predictions = model.transform(df_with_features)
        
        # Extraire probabilités
        from pyspark.sql.functions import udf
        from pyspark.sql.types import DoubleType
        
        @udf(returnType=DoubleType())
        def extract_fraud_prob(probability):
            if probability and len(probability) > 1:
                return float(probability[1])
            return 0.0
        
        predictions = predictions.withColumn("fraud_probability", extract_fraud_prob(col("probability")))
        predictions = predictions.withColumn("fraud_score", col("fraud_probability") * 100)
        predictions = predictions.withColumnRenamed("prediction", "predicted_fraud")
        
        # Niveau de risque
        predictions = predictions.withColumn(
            "risk_level",
            when(col("fraud_score") >= 70, "HIGH")
            .when(col("fraud_score") >= 50, "MEDIUM")
            .when(col("fraud_score") >= 30, "LOW")
            .otherwise("SAFE")
        )
        
        print("*** Prédictions calculées")
        return predictions
        
    except Exception as e:
        print(f"!!! Erreur: {e}")
        raise


def main():
    print("="*60)
    print("* TEST DU MODELE ML (SANS KAFKA)")
    print("="*60)
    
    # Créer Spark
    spark = create_simple_spark()
    print(f"*** Spark {spark.version} démarré\n")
    
    # Chemin du modèle
    model_path = "./data/models/random_forest_fraud_detector"
    
    if not Path(model_path).exists():
        print(f"!!! Modèle introuvable: {model_path}")
        print("Assurez-vous d'avoir entraîné le modèle d'abord.")
        return
    
    # Créer données de test
    df = create_test_data(spark)
    
    # Créer features
    df_features = create_features(df)
    
    # Appliquer modèle
    predictions = apply_model(df_features, model_path)
    
    # Afficher résultats
    print("\n" + "="*60)
    print("* RÉSULTATS DES PRÉDICTIONS")
    print("="*60)
    
    predictions.select(
        "transaction_id",
        "amount",
        "merchant_category",
        "hour_of_day",
        "is_fraud",
        "predicted_fraud",
        "fraud_score",
        "risk_level"
    ).show(truncate=False)
    
    # Statistiques
    print("\n* STATISTIQUES:")
    total = predictions.count()
    frauds_detected = predictions.filter(col("predicted_fraud") == 1.0).count()
    actual_frauds = predictions.filter(col("is_fraud") == 1).count()
    
    print(f"  Total transactions: {total}")
    print(f"  Fraudes réelles: {actual_frauds}")
    print(f"  Fraudes détectées: {frauds_detected}")
    
    # Matrice de confusion
    tp = predictions.filter((col("is_fraud") == 1) & (col("predicted_fraud") == 1.0)).count()
    fp = predictions.filter((col("is_fraud") == 0) & (col("predicted_fraud") == 1.0)).count()
    tn = predictions.filter((col("is_fraud") == 0) & (col("predicted_fraud") == 0.0)).count()
    fn = predictions.filter((col("is_fraud") == 1) & (col("predicted_fraud") == 0.0)).count()
    
    print("\n* MATRICE DE CONFUSION:")
    print(f"  True Positives:  {tp}")
    print(f"  False Positives: {fp}")
    print(f"  True Negatives:  {tn}")
    print(f"  False Negatives: {fn}")
    
    if (tp + fp) > 0:
        precision = tp / (tp + fp)
        print(f"\n  Précision: {precision:.2%}")
    
    if (tp + fn) > 0:
        recall = tp / (tp + fn)
        print(f"  Rappel: {recall:.2%}")
    
    print("\n*** Test terminé avec succès!")
    spark.stop()

if __name__ == "__main__":
    main()