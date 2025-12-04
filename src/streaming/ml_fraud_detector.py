"""
Détecteur de fraudes en temps réel avec Spark Streaming + Modèle ML.
Consomme les transactions depuis Kafka, calcule les features, et détecte 
les fraudes grâce au modèle ML préentraîné.
"""

import os
import sys
from pathlib import Path
from pyspark.sql.functions import col, when, lit
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler

# Ajuster le path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.utils.spark_utils import (
    create_spark_session,
    read_kafka_stream,
    parse_kafka_message,
    write_to_console,
    write_to_parquet,
    write_to_memory
)
from src.utils.logger import setup_logger
from dotenv import load_dotenv

load_dotenv()
logger = setup_logger(__name__)


class MlFraudDetector:
    """
    Détecteur de fraude en temps réel avec Spark Streaming, basé sur 
    un modèle ML préentraîné.
    """
    
    def __init__(self):
        """Initialise le détecteur de fraudes."""
        logger.info(" Initialisation du détecteur de fraudes ML")
        
        # Créer la session Spark
        self.spark = create_spark_session()
        
        # Chemins de sortie
        self.data_path = os.getenv('DATA_PATH', './data')
        self.output_path = os.path.join(self.data_path, 'transactions')
        self.checkpoint_path = os.path.join(self.data_path, 'checkpoints')
        
        # Créer les dossiers si nécessaire
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        Path(self.checkpoint_path).mkdir(parents=True, exist_ok=True)

        # Chemin du modèle préentraîné
        self.model_path = os.path.join(self.data_path, 'models', 'random_forest_fraud_detector')
        
        # Features utilisées par le modèle (MEME ordre que l'entraînement)
        self.feature_columns = [
            'amount', 'amount_log', 'hour_of_day', 'day_of_week',
            'is_weekend', 'is_unusual_hour', 'is_high_amount',
            'is_round_amount', 'is_high_risk_merchant', 'is_online_int',
            'location_lat', 'location_lon'
        ]
        
        logger.info(f" Détecteur initialisé - Output: {self.output_path}")
        logger.info(f" Modèle: {self.model_path}")

    def _create_features(self, df):
        """
        Crée les features nécessaires pour le modèle ML.
        IMPORTANT: Garde TOUTES les colonnes originales !
        
        Args:
            df: DataFrame original avec les transactions
            
        Returns:
            DataFrame avec les features ajoutées (sans perdre les colonnes originales)
        """
        from pyspark.sql.functions import to_timestamp, hour, dayofweek, log10
        
        logger.info(" Création des features pour le modèle")
        
        # Convertir timestamp
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
        
        logger.info(f" {len(self.feature_columns)} features créées")
        return df

    def _apply_ml_model(self, df):
        """
        Applique le modèle ML sur le DataFrame.
        CRITIQUE: Garde toutes les colonnes originales + ajoute les prédictions.
        
        Args:
            df: DataFrame avec features calculées
            
        Returns:
            DataFrame avec les prédictions du modèle ET toutes les colonnes originales
        """
        logger.info(" Application du modèle ML")
        
        try:
            # Charger le modèle
            logger.info(f"** Chargement du modèle depuis: {self.model_path}")
            model = RandomForestClassificationModel.load(self.model_path)
            
            # Assembler les features SANS perdre les autres colonnes
            assembler = VectorAssembler(inputCols=self.feature_columns, outputCol="features")
            df_with_features = assembler.transform(df)
            
            # Appliquer le modèle (ajoute: rawPrediction, probability, prediction)
            predictions = model.transform(df_with_features)
            
            # Renommer 'prediction' en 'predicted_fraud' pour cohérence
            predictions = predictions.withColumnRenamed("prediction", "predicted_fraud")
            
            # Extraire le score de fraude (probabilité de la classe 1)
            from pyspark.sql.functions import udf
            from pyspark.sql.types import DoubleType
            
            # UDF pour extraire la probabilité de fraude
            @udf(returnType=DoubleType())
            def extract_fraud_probability(probability):
                if probability and len(probability) > 1:
                    return float(probability[1])  # Probabilité de la classe 1 (fraude)
                return 0.0
            
            predictions = predictions.withColumn("fraud_probability", extract_fraud_probability(col("probability")))
            
            # Calculer fraud_score (0-100)
            predictions = predictions.withColumn("fraud_score", col("fraud_probability") * 100)
            
            # Calculer le niveau de risque
            predictions = predictions.withColumn(
                "risk_level",
                when(col("fraud_score") >= 70, "HIGH")
                .when(col("fraud_score") >= 50, "MEDIUM")
                .when(col("fraud_score") >= 30, "LOW")
                .otherwise("SAFE")
            )
            
            logger.info(" Modèle appliqué avec succès")
            return predictions
            
        except Exception as e:
            logger.error(f" Erreur lors de l'application du modèle: {e}")
            raise
    
    def _calculate_metrics(self, df):
        """
        Calcule des métriques de performance (pour monitoring).
        
        Args:
            df: DataFrame avec is_fraud et predicted_fraud
            
        Returns:
            DataFrame avec métriques
        """
        # Ajouter des colonnes pour les métriques
        df = df.withColumn(
            'true_positive',
            when((col('is_fraud') == 1) & (col('predicted_fraud') == 1.0), 1).otherwise(0)
        )
        
        df = df.withColumn(
            'false_positive',
            when((col('is_fraud') == 0) & (col('predicted_fraud') == 1.0), 1).otherwise(0)
        )
        
        df = df.withColumn(
            'true_negative',
            when((col('is_fraud') == 0) & (col('predicted_fraud') == 0.0), 1).otherwise(0)
        )
        
        df = df.withColumn(
            'false_negative',
            when((col('is_fraud') == 1) & (col('predicted_fraud') == 0.0), 1).otherwise(0)
        )
        
        return df
    
    def run(self, mode: str = "console", trigger_interval: str = "5 seconds"):
        """
        Lance le pipeline de détection en temps réel.
        
        Args:
            mode: Mode de sortie ('console', 'parquet', 'memory', 'all')
            trigger_interval: Intervalle de traitement
        """
        logger.info(f" Démarrage du pipeline ML - Mode: {mode}")
        
        try:
            # 1. Lire le stream Kafka
            logger.info(" Connexion au stream Kafka...")
            raw_stream = read_kafka_stream(self.spark)
            
            # 2. Parser les messages JSON
            logger.info(" Parsing des messages...")
            parsed_stream = parse_kafka_message(raw_stream)
            
            # 3. Créer les features
            logger.info(" Création des features...")
            enriched_stream = self._create_features(parsed_stream)
            
            # 4. Appliquer le modèle ML
            logger.info(" Application du modèle ML...")
            detected_stream = self._apply_ml_model(enriched_stream)
            
            # 5. Calculer les métriques
            detected_stream = self._calculate_metrics(detected_stream)
            
            # 6. Sélectionner les colonnes importantes pour la sortie
            # IMPORTANT: On garde toutes les colonnes nécessaires pour le dashboard
            output_stream = detected_stream.select(
                # Identifiants
                'transaction_id',
                'user_id',
                'timestamp',
                
                # Détails transaction
                'amount',
                'merchant_id',
                'merchant_category',
                'is_online',
                
                # Localisation (pour la carte)
                'location_lat',
                'location_lon',
                
                # Features temporelles
                'hour_of_day',
                'day_of_week',
                'is_unusual_hour',
                'is_weekend',
                
                # Label et prédiction
                'is_fraud',          # Label réel
                'predicted_fraud',   # Prédiction ML (0 ou 1)
                'fraud_probability', # Probabilité (0-1)
                'fraud_score',       # Score (0-100)
                'risk_level',        # SAFE/LOW/MEDIUM/HIGH
                
                # Métriques
                'true_positive',
                'false_positive',
                'true_negative',
                'false_negative'
            )
            
            # 7. Écrire selon le mode choisi
            queries = []
            
            if mode in ['console', 'all']:
                logger.info(" Démarrage du stream vers console...")
                console_query = write_to_console(
                    output_stream.select(
                        'transaction_id', 'user_id', 'amount', 
                        'is_fraud', 'predicted_fraud', 'fraud_score', 'risk_level'
                    ),
                    trigger=trigger_interval
                )
                queries.append(console_query)
            
            if mode in ['parquet', 'all']:
                logger.info(" Démarrage du stream vers Parquet...")
                parquet_query = write_to_parquet(
                    output_stream,
                    self.output_path,
                    self.checkpoint_path,
                    trigger=trigger_interval
                )
                queries.append(parquet_query)
            
            if mode in ['memory', 'all']:
                logger.info(" Démarrage du stream vers mémoire (pour dashboard)...")
                memory_query = write_to_memory(
                    output_stream,
                    table_name='fraud_detection_ml',
                    trigger=trigger_interval
                )
                queries.append(memory_query)
            
            # 8. Attendre l'arrêt
            logger.info(" Pipeline ML démarré avec succès")
            logger.info("  Appuyez sur Ctrl+C pour arrêter")
            
            # Attendre que toutes les queries se terminent
            for query in queries:
                query.awaitTermination()
                
        except KeyboardInterrupt:
            logger.info("\n Arrêt demandé par l'utilisateur")
            
        except Exception as e:
            logger.error(f" Erreur dans le pipeline: {e}", exc_info=True)
            
        finally:
            logger.info(" Arrêt du pipeline")
            self.spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Détecteur de fraudes ML en temps réel')
    parser.add_argument(
        '--mode',
        choices=['console', 'parquet', 'memory', 'all'],
        default='memory',  # Par défaut memory pour le dashboard
        help='Mode de sortie des résultats'
    )
    parser.add_argument(
        '--trigger',
        default='5 seconds',
        help='Intervalle de traitement (ex: "5 seconds", "1 minute")'
    )
    
    args = parser.parse_args()
    
    # Créer et lancer le détecteur ML
    detector = MlFraudDetector()
    detector.run(mode=args.mode, trigger_interval=args.trigger)