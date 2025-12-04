"""
Détecteur de fraudes en temps réel avec Spark Streaming.
Consomme les transactions depuis Kafka, calcule les features, et détecte les fraudes.
"""

import os
import sys
from pathlib import Path
from pyspark.sql.functions import col, when, lit, struct, to_json

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
from src.models.feature_engineering import FraudFeatureEngineering
from src.utils.logger import setup_logger
from dotenv import load_dotenv

load_dotenv()
logger = setup_logger(__name__)


class FraudDetector:
    """
    Système de détection de fraudes en temps réel.
    """
    
    def __init__(self):
        """Initialise le détecteur de fraudes."""
        logger.info("******** Initialisation du détecteur de fraudes **********")
        
        # Créer la session Spark
        self.spark = create_spark_session()
        
        # Chemins de sortie
        self.data_path = os.getenv('DATA_PATH', './data')
        self.output_path = os.path.join(self.data_path, 'transactions')
        self.checkpoint_path = os.path.join(self.data_path, 'checkpoints')
        
        # Créer les dossiers si nécessaire
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        Path(self.checkpoint_path).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"****** Détecteur initialisé - Output: {self.output_path} *******")
    
    def _apply_rule_based_detection(self, df):
        """
        Applique des règles simples de détection de fraude.
        Ces règles servent de baseline avant le ML.
        
        Args:
            df: DataFrame avec features calculées
            
        Returns:
            DataFrame avec colonne 'fraud_score' et 'predicted_fraud'
        """
        logger.info("Application des règles de détection")
        
        # Initialiser le score à 0
        df = df.withColumn('fraud_score', lit(0.0))
        
        # Règle 1: Montant très élevé (+30 points)
        df = df.withColumn(
            'fraud_score',
            when(col('amount') > 500, col('fraud_score') + 30)
            .otherwise(col('fraud_score'))
        )
        
        # Règle 2: Heure inhabituelle (+20 points)
        df = df.withColumn(
            'fraud_score',
            when(col('is_unusual_hour') == 1, col('fraud_score') + 20)
            .otherwise(col('fraud_score'))
        )
        
        # Règle 3: Transactions rapides (+25 points)
        df = df.withColumn(
            'fraud_score',
            when(col('is_rapid_transaction') == 1, col('fraud_score') + 25)
            .otherwise(col('fraud_score'))
        )
        
        # Règle 4: Déplacement impossible (+40 points - fort indicateur)
        df = df.withColumn(
            'fraud_score',
            when(col('is_impossible_travel') == 1, col('fraud_score') + 40)
            .otherwise(col('fraud_score'))
        )
        
        # Règle 5: Changement de device + montant élevé (+30 points)
        df = df.withColumn(
            'fraud_score',
            when(
                (col('device_changed') == 1) & (col('amount') > 200),
                col('fraud_score') + 30
            ).otherwise(col('fraud_score'))
        )
        
        # Règle 6: Marchand à haut risque + montant élevé (+15 points)
        df = df.withColumn(
            'fraud_score',
            when(
                (col('is_high_risk_merchant') == 1) & (col('amount') > 300),
                col('fraud_score') + 15
            ).otherwise(col('fraud_score'))
        )
        
        # Règle 7: Beaucoup de transactions en peu de temps (+20 points)
        df = df.withColumn(
            'fraud_score',
            when(col('txn_count_5min') > 3, col('fraud_score') + 20)
            .otherwise(col('fraud_score'))
        )
        
        # Prédiction finale: fraude si score >= 50
        df = df.withColumn(
            'predicted_fraud',
            when(col('fraud_score') >= 50, 1).otherwise(0)
        )
        
        # Niveau de risque
        df = df.withColumn(
            'risk_level',
            when(col('fraud_score') >= 70, 'HIGH')
            .when(col('fraud_score') >= 50, 'MEDIUM')
            .when(col('fraud_score') >= 30, 'LOW')
            .otherwise('SAFE')
        )
        
        return df
    
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
            when((col('is_fraud') == 1) & (col('predicted_fraud') == 1), 1).otherwise(0)
        )
        
        df = df.withColumn(
            'false_positive',
            when((col('is_fraud') == 0) & (col('predicted_fraud') == 1), 1).otherwise(0)
        )
        
        df = df.withColumn(
            'true_negative',
            when((col('is_fraud') == 0) & (col('predicted_fraud') == 0), 1).otherwise(0)
        )
        
        df = df.withColumn(
            'false_negative',
            when((col('is_fraud') == 1) & (col('predicted_fraud') == 0), 1).otherwise(0)
        )
        
        return df
    
    def run(self, mode: str = "console", trigger_interval: str = "5 seconds"):
        """
        Lance le pipeline de détection en temps réel.
        
        Args:
            mode: Mode de sortie ('console', 'parquet', 'memory', 'all')
            trigger_interval: Intervalle de traitement
        """
        logger.info(f"----*****---- Démarrage du pipeline de détection - Mode: {mode} --------*****--------")
        
        try:
            # 1. Lire le stream Kafka
            logger.info("* Connexion au stream Kafka...")
            raw_stream = read_kafka_stream(self.spark)
            
            # 2. Parser les messages JSON
            logger.info("* Parsing des messages...")
            parsed_stream = parse_kafka_message(raw_stream)
            
            # 3. Appliquer le feature engineering
            logger.info("* Application du feature engineering...")
            enriched_stream = FraudFeatureEngineering.create_all_features(parsed_stream)
            
            # 4. Détection de fraude (règles)
            logger.info("* Application de la détection de fraude...")
            detected_stream = self._apply_rule_based_detection(enriched_stream)
            
            # 5. Calculer les métriques
            detected_stream = self._calculate_metrics(detected_stream)
            
            # 6. Sélectionner les colonnes importantes pour la sortie
            output_stream = detected_stream.select(
                'transaction_id',
                'user_id',
                'timestamp',
                'amount',
                'merchant_category',
                'is_online',
                'is_fraud',  # Label réel
                'predicted_fraud',  # Prédiction
                'fraud_score',
                'risk_level',
                'hour_of_day',
                'is_unusual_hour',
                'is_rapid_transaction',
                'is_impossible_travel',
                'device_changed',
                'time_since_last_txn',
                'distance_from_prev',
                'true_positive',
                'false_positive',
                'true_negative',
                'false_negative'
            )
            
            # 7. Écrire selon le mode choisi
            queries = []
            
            if mode in ['console', 'all']:
                logger.info("****** Démarrage du stream vers console... *******")
                console_query = write_to_console(
                    output_stream.select(
                        'transaction_id', 'user_id', 'amount', 
                        'is_fraud', 'predicted_fraud', 'fraud_score', 'risk_level'
                    ),
                    trigger=trigger_interval
                )
                queries.append(console_query)
            
            if mode in ['parquet', 'all']:
                logger.info("******** Démarrage du stream vers Parquet... ********")
                parquet_query = write_to_parquet(
                    output_stream,
                    self.output_path,
                    self.checkpoint_path,
                    trigger=trigger_interval
                )
                queries.append(parquet_query)
            
            if mode in ['memory', 'all']:
                logger.info("******* Démarrage du stream vers mémoire... *******")
                memory_query = write_to_memory(
                    output_stream,
                    table_name='fraud_detection',
                    trigger=trigger_interval
                )
                queries.append(memory_query)
            
            # 8. Attendre l'arrêt
            logger.info("* Pipeline démarré avec succès")
            logger.info("**  Appuyez sur Ctrl+C pour arrêter")
            
            # Attendre que toutes les queries se terminent
            for query in queries:
                query.awaitTermination()
                
        except KeyboardInterrupt:
            logger.info("\n !------! Arrêt demandé par l'utilisateur !--------!")
            
        except Exception as e:
            logger.error(f"!!!!!!!! Erreur dans le pipeline: {e} !!!!!!!!!!", exc_info=True)
            
        finally:
            logger.info(" !!!!!!!!!!! Arrêt du pipeline !!!!!!!!!!")
            self.spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Détecteur de fraudes en temps réel')
    parser.add_argument(
        '--mode',
        choices=['console', 'parquet', 'memory', 'all'],
        default='console',
        help='Mode de sortie des résultats'
    )
    parser.add_argument(
        '--trigger',
        default='5 seconds',
        help='Intervalle de traitement (ex: "5 seconds", "1 minute")'
    )
    
    args = parser.parse_args()
    
    # Créer et lancer le détecteur
    detector = FraudDetector()
    detector.run(mode=args.mode, trigger_interval=args.trigger)