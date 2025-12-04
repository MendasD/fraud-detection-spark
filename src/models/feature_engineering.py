"""
Feature Engineering pour la détection de fraudes.
Calcule les features à partir des transactions en temps réel.
VERSION STREAMING - Sans fonctions window non supportées.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, count, avg, stddev, max as spark_max, min as spark_min,
    unix_timestamp, lit, sqrt, pow as spark_pow, hour, dayofweek,
    sum as spark_sum, abs as spark_abs, expr, log10, to_timestamp
)
from pyspark.ml.feature import VectorAssembler
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
    VERSION ADAPTEE POUR STREAMING (sans lag/window functions).
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
        
        df = df.withColumn('hour_of_day', hour(col('timestamp')))
        df = df.withColumn('day_of_week', dayofweek(col('timestamp')))
        
        # Flag pour heures inhabituelles (2h-5h du matin)
        df = df.withColumn(
            'is_unusual_hour',
            when((col('hour_of_day') >= 2) & (col('hour_of_day') <= 5), 1).otherwise(0)
        )
        
        # Flag pour week-end
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
        df = df.withColumn(
            'amount_log', 
            when(col('amount') > 0, expr('log10(amount)')).otherwise(0)
        )
        
        # Catégories de montant
        df = df.withColumn(
            'amount_category',
            when(col('amount') < 20, 'low')
            .when((col('amount') >= 20) & (col('amount') < 100), 'medium')
            .when((col('amount') >= 100) & (col('amount') < 500), 'high')
            .otherwise('very_high')
        )
        
        # Flags simples pour montants suspects
        df = df.withColumn('is_high_amount', when(col('amount') > 500, 1).otherwise(0))
        df = df.withColumn('is_very_high_amount', when(col('amount') > 1000, 1).otherwise(0))
        
        return df
    
    @staticmethod
    def add_simple_features(df: DataFrame) -> DataFrame:
        """
        Ajoute des features simples qui ne nécessitent pas de window functions.
        Ces features sont compatibles avec le streaming.
        
        Args:
            df: DataFrame avec colonnes de base
            
        Returns:
            DataFrame avec features simples
        """
        logger.info("Ajout des features simples (compatibles streaming)")
        
        # Flag pour montant rond (souvent suspect)
        df = df.withColumn(
            'is_round_amount',
            when(col('amount') % 10 == 0, 1).otherwise(0)
        )
        
        # Flag pour transaction en ligne
        df = df.withColumn(
            'is_online_flag',
            when(col('is_online') == True, 1).otherwise(0)
        )
        
        # Timestamp en secondes (epoch)
        df = df.withColumn('timestamp_epoch', unix_timestamp('timestamp'))
        
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
        
        # Flag pour achats en ligne
        df = df.withColumn(
            'is_online_merchant',
            when(col('merchant_category') == 'online_shopping', 1).otherwise(0)
        )
        
        return df
    
    @staticmethod
    def create_all_features(df: DataFrame) -> DataFrame:
        """
        Applique toutes les transformations de features.
        VERSION STREAMING - Sans agrégations complexes.
        
        Args:
            df: DataFrame brut des transactions
            
        Returns:
            DataFrame avec toutes les features calculées
        """
        logger.info(" Début du feature engineering complet (VERSION STREAMING)")
        
        # Appliquer toutes les transformations compatibles streaming
        df = FraudFeatureEngineering.add_time_features(df)
        df = FraudFeatureEngineering.add_amount_features(df)
        df = FraudFeatureEngineering.add_simple_features(df)
        df = FraudFeatureEngineering.add_merchant_features(df)
        
        logger.info(" Feature engineering terminé (VERSION STREAMING)")
        
        return df
    
    @staticmethod
    def create_features_for_model(df: DataFrame) -> DataFrame:
        """
        Applique toutes les transformations de features pour le modèle ML.
        VERSION STREAMING - Features simplifiées.

        Args:
            df: DataFrame brut des transactions
        """

        # Convertir timestamp en datetime
        df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

        # Features temporelles
        df = df.withColumn("hour_of_day", hour(col("timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("timestamp"))) \
            .withColumn("is_weekend", when((dayofweek("timestamp") == 1) | (dayofweek("timestamp") == 7), 1).otherwise(0)) \
            .withColumn("is_unusual_hour", when((hour("timestamp") >= 2) & (hour("timestamp") <= 5), 1).otherwise(0))

        # Features par montant
        df = df.withColumn("amount_log", log10(col("amount") + 1)) \
            .withColumn("is_high_amount", when(col("amount") > 500, 1).otherwise(0)) \
            .withColumn("is_round_amount", when(col("amount") % 10 == 0, 1).otherwise(0))

        # Features marchand
        df = df.withColumn("is_high_risk_merchant", 
                        when(col("merchant_category").isin(['electronics', 'online_shopping', 'airline', 'hotel']), 1)
                        .otherwise(0))

        # Features online/offline
        df = df.withColumn("is_online_int", when(col("is_online") == True, 1).otherwise(0))

        logger.info("*** Features créées ***")

        return df
    
    @staticmethod
    def assemble_features(df: DataFrame) -> DataFrame:
        """
        Assemble les features pour le modèle ML.
    
        Args:
            df: DataFrame avec toutes les features

        Returns:
            DataFrame avec features assemblees
        """
        logger.info("Assemblage des features")

        # Sélection des colonnes pour le modèle
        feature_columns = [
                            'amount', 'amount_log', 'hour_of_day', 'day_of_week',
                            'is_weekend', 'is_unusual_hour', 'is_high_amount',
                            'is_round_amount', 'is_high_risk_merchant', 'is_online_int',
                            'location_lat', 'location_lon'
                        ]
        # Assemblage des features
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

        # Renommer la colonne target
        df = df.withColumnRenamed("is_fraud", "label")

        # Appliquer l'assembleur
        df_ml = assembler.transform(df).select("features", "label")

        logger.info("Assemblage des features terminé")

        return df_ml
    
    @staticmethod
    def get_feature_columns() -> List[str]:
        """
        Retourne la liste des colonnes de features pour le modèle ML.
        VERSION STREAMING - Features simplifiées.
        
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
            'is_high_amount',
            'is_very_high_amount',
            'is_round_amount',
            
            # Features marchand
            'is_high_risk_merchant',
            'is_online_merchant',
            
            # Features online/offline
            'is_online',
            'is_online_flag',
        ]


if __name__ == "__main__":
    print(" Module de feature engineering chargé (VERSION STREAMING)")
    print(f"\n Features disponibles ({len(FraudFeatureEngineering.get_feature_columns())}):")
    for i, feature in enumerate(FraudFeatureEngineering.get_feature_columns(), 1):
        print(f"  {i}. {feature}")