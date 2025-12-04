"""
Feature Engineering pour la détection de fraudes.
Calcule les features à partir des transactions en temps réel.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, count, avg, stddev, max as spark_max, min as spark_min,
    unix_timestamp, lag, lit, sqrt, pow as spark_pow, hour, dayofweek,
    sum as spark_sum, abs as spark_abs
)
from pyspark.sql.window import Window
from typing import List
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class FraudFeatureEngineering:
    """
    Classe pour générer les features de détection de fraude.
    Features calculées en temps réel sur les streams Spark.
    """
    
    @staticmethod
    def add_time_features(df: DataFrame) -> DataFrame:
        """
        Ajoute des features temporelles.
        
        Args:
            df: DataFrame avec colonne 'timestamp'
            
        Returns:
            DataFrame avec features temporelles ajoutées
        """
        logger.info("Ajout des features temporelles")
        
        # Créer les colonnes pour l'heure du jour et le jour de la semaine
        df = df.withColumn('hour_of_day', hour(col('timestamp')))
        df = df.withColumn('day_of_week', dayofweek(col('timestamp')))
        
        # Flag pour heures inhabituelles (2h-5h du matin)
        df = df.withColumn(
            'is_unusual_hour',
            when((col('hour_of_day') >= 2) & (col('hour_of_day') <= 5), 1).otherwise(0)
        )
        
        # Flag pour week-end (1 = dimanche et 7 = samedi)
        df = df.withColumn(
            'is_weekend',
            when((col('day_of_week') == 1) | (col('day_of_week') == 7), 1).otherwise(0)
        )
        
        return df
    
    @staticmethod
    def add_amount_features(df: DataFrame) -> DataFrame:
        """
        Ajoute des features basées sur le montant.
        
        Args:
            df: DataFrame avec colonne 'amount'
            
        Returns:
            DataFrame avec features de montant ajoutées
        """
        logger.info("Ajout des features de montant")
        
        # Montant en log (pour normaliser les grandes valeurs)
        df = df.withColumn('amount_log', when(col('amount') > 0, 
                                              spark_pow(lit(10), col('amount'))).otherwise(0))
        
        # Catégories de montant
        df = df.withColumn(
            'amount_category',
            when(col('amount') < 20, 'low')
            .when((col('amount') >= 20) & (col('amount') < 100), 'medium')
            .when((col('amount') >= 100) & (col('amount') < 500), 'high')
            .otherwise('very_high')
        )
        
        return df
    
    @staticmethod
    def add_user_aggregations(df: DataFrame, window_duration: str = '1 hour') -> DataFrame:
        """
        Ajoute des agrégations par utilisateur (fenêtres glissantes).
        
        Args:
            df: DataFrame avec colonnes user_id, timestamp, amount
            window_duration: Durée de la fenêtre (ex: '1 hour', '30 minutes')
            
        Returns:
            DataFrame avec agrégations utilisateur
        """
        logger.info(f"Ajout des agrégations utilisateur (fenêtre: {window_duration})")
        
        # Définir la fenêtre temporelle par utilisateur
        from pyspark.sql.functions import window as time_window
        
        # Agrégations sur fenêtre glissante
        user_stats = df.groupBy(
            'user_id',
            time_window('timestamp', window_duration)
        ).agg(
            count('transaction_id').alias('txn_count_window'),
            avg('amount').alias('avg_amount_window'),
            spark_max('amount').alias('max_amount_window'),
            spark_min('amount').alias('min_amount_window'),
            stddev('amount').alias('std_amount_window'),
            spark_sum('amount').alias('total_amount_window')
        )
        
        # Joindre avec le DataFrame original
        df = df.join(
            user_stats,
            on='user_id',
            how='left'
        )
        
        # Remplacer les nulls par des valeurs par défaut
        df = df.fillna({
            'txn_count_window': 0,
            'avg_amount_window': 0,
            'max_amount_window': 0,
            'min_amount_window': 0,
            'std_amount_window': 0,
            'total_amount_window': 0
        })
        
        return df
    
    @staticmethod
    def add_velocity_features(df: DataFrame) -> DataFrame:
        """
        Ajoute des features de vélocité (fréquence des transactions).
        Utilise les fenêtres Windows pour calculer le temps entre transactions.
        
        Args:
            df: DataFrame avec user_id, timestamp
            
        Returns:
            DataFrame avec features de vélocité
        """
        logger.info("Ajout des features de vélocité")
        
        # Convertir timestamp en secondes (epoch) pour les calculs de fenêtre
        df = df.withColumn('timestamp_seconds', unix_timestamp('timestamp'))
        
        # Fenêtre partitionnée par user_id, ordonnée par timestamp
        window_spec = Window.partitionBy('user_id').orderBy('timestamp')
        
        # Fenêtre sur les secondes (pour rangeBetween)
        window_spec_seconds = Window.partitionBy('user_id').orderBy('timestamp_seconds')
        
        # Temps depuis la dernière transaction (en secondes)
        df = df.withColumn(
            'prev_timestamp',
            lag('timestamp', 1).over(window_spec) # temps de la transaction précédente
        )
        
        df = df.withColumn(
            'time_since_last_txn',
            when(
                col('prev_timestamp').isNotNull(),
                unix_timestamp('timestamp') - unix_timestamp('prev_timestamp')
            ).otherwise(999999)  # Valeur élevée si première transaction
        )
        
        # Flag pour transactions rapides (< 60 secondes)
        df = df.withColumn(
            'is_rapid_transaction',
            when(col('time_since_last_txn') < 60, 1).otherwise(0)
        )
        
        # Nombre de transactions dans les dernières 5 minutes
        df = df.withColumn(
            'txn_count_5min',
            count('transaction_id').over(
                window_spec_seconds.rangeBetween(-300, 0)  # 300 secondes = 5 minutes
            )
        )
        
        return df
    
    @staticmethod
    def add_location_features(df: DataFrame) -> DataFrame:
        """
        Ajoute des features géographiques.
        
        Args:
            df: DataFrame avec location_lat, location_lon
            
        Returns:
            DataFrame avec features de localisation
        """
        logger.info("Ajout des features de localisation")
        
        # Distance depuis la dernière transaction (approximation simple)
        window_spec = Window.partitionBy('user_id').orderBy('timestamp')
        
        df = df.withColumn('prev_lat', lag('location_lat', 1).over(window_spec))
        df = df.withColumn('prev_lon', lag('location_lon', 1).over(window_spec))
        
        # Calcul distance euclidienne approximative (en degrés)
        # Pour une meilleure approximation, on pourrait utiliser la formule de Haversine
        df = df.withColumn(
            'distance_from_prev',
            when(
                col('prev_lat').isNotNull(),
                sqrt(
                    spark_pow(col('location_lat') - col('prev_lat'), 2) +
                    spark_pow(col('location_lon') - col('prev_lon'), 2)
                ) * 111  # Conversion degrés -> km (approximation)
            ).otherwise(0)
        )
        
        # Flag pour déplacement suspect (> 100km entre transactions rapides)
        df = df.withColumn(
            'is_impossible_travel',
            when(
                (col('distance_from_prev') > 100) & 
                (col('time_since_last_txn') < 3600),  # < 1 heure
                1
            ).otherwise(0)
        )
        
        return df
    
    @staticmethod
    def add_device_features(df: DataFrame) -> DataFrame:
        """
        Ajoute des features liées au device.
        
        Args:
            df: DataFrame avec device_id, user_id
            
        Returns:
            DataFrame avec features de device
        """
        logger.info("Ajout des features de device")
        
        # Compter le nombre de devices différents par utilisateur (fenêtre)
        window_spec = Window.partitionBy('user_id').orderBy('timestamp')
        
        df = df.withColumn('prev_device', lag('device_id', 1).over(window_spec))
        
        # Flag pour changement de device
        df = df.withColumn(
            'device_changed',
            when(
                (col('prev_device').isNotNull()) & 
                (col('device_id') != col('prev_device')),
                1
            ).otherwise(0)
        )
        
        return df
    
    @staticmethod
    def add_merchant_features(df: DataFrame) -> DataFrame:
        """
        Ajoute des features liées au marchand.
        
        Args:
            df: DataFrame avec merchant_category
            
        Returns:
            DataFrame avec features marchand
        """
        logger.info("Ajout des features marchand")
        
        # Flag pour catégories à haut risque
        high_risk_categories = ['electronics', 'online_shopping', 'airline', 'hotel']
        
        df = df.withColumn(
            'is_high_risk_merchant',
            when(col('merchant_category').isin(high_risk_categories), 1).otherwise(0)
        )
        
        return df
    
    @staticmethod
    def create_all_features(df: DataFrame) -> DataFrame:
        """
        Applique toutes les transformations de features.
        
        Args:
            df: DataFrame brut des transactions
            
        Returns:
            DataFrame avec toutes les features calculées
        """
        logger.info("###### Début du feature engineering complet ######")
        
        # Appliquer toutes les transformations
        df = FraudFeatureEngineering.add_time_features(df)
        df = FraudFeatureEngineering.add_amount_features(df)
        df = FraudFeatureEngineering.add_velocity_features(df)
        df = FraudFeatureEngineering.add_location_features(df)
        df = FraudFeatureEngineering.add_device_features(df)
        df = FraudFeatureEngineering.add_merchant_features(df)
        
        logger.info(" ###### Feature engineering terminé ######")
        
        return df
    
    @staticmethod
    def get_feature_columns() -> List[str]:
        """
        Retourne la liste des colonnes de features pour le modèle ML.
        
        Returns:
            Liste des noms de colonnes
        """
        return [
            # Features temporelles
            'hour_of_day',
            'day_of_week',
            'is_unusual_hour',
            'is_weekend',
            
            # Features de montant
            'amount',
            'amount_log',
            
            # Features de vélocité
            'time_since_last_txn',
            'is_rapid_transaction',
            'txn_count_5min',
            
            # Features de localisation
            'distance_from_prev',
            'is_impossible_travel',
            
            # Features de device
            'device_changed',
            
            # Features marchand
            'is_high_risk_merchant',
            
            # Features online/offline
            'is_online',
        ]


if __name__ == "__main__":
    print(" Module de feature engineering chargé")
    print(f"\n Features disponibles ({len(FraudFeatureEngineering.get_feature_columns())}):")
    for i, feature in enumerate(FraudFeatureEngineering.get_feature_columns(), 1):
        print(f"  {i}. {feature}")