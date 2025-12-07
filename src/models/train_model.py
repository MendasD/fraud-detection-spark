"""
Script d'entraînement du modèle de détection de fraudes avec Spark MLlib.
Traite 13 millions de transactions en mode distribué.
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Configuration HADOOP pour Windows (AVANT les imports PySpark)
hadoop_home = os.getenv('HADOOP_HOME')
if Path(hadoop_home).exists():
    os.environ['HADOOP_HOME'] = hadoop_home
    bin_path = os.path.join(hadoop_home, 'bin')
    if bin_path not in os.environ.get('PATH', ''):
        os.environ['PATH'] = f"{bin_path};{os.environ.get('PATH', '')}"

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class FraudModelTrainer:
    """Entraîneur de modèle ML pour la détection de fraudes."""
    
    def __init__(self, data_path: str = "./data/historical"):
        self.data_path = data_path
        self.feature_columns = [
            'amount', 'amount_log', 'hour_of_day', 'day_of_week',
            'is_weekend', 'is_unusual_hour', 'is_high_amount',
            'is_round_amount', 'is_high_risk_merchant', 'is_online_int',
            'location_lat', 'location_lon'
        ]
        
        # Créer la session Spark
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self):
        """Crée une session Spark optimisée pour ML."""
        logger.info("Création de la session Spark...")
        
        spark = SparkSession.builder \
            .appName("FraudDetectionTraining") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"✅ Spark {spark.version} démarré")
        return spark
    
    def load_data(self):
        """Charge les données historiques."""
        logger.info(f"📥 Chargement des données depuis: {self.data_path}")
        
        df = self.spark.read.parquet(self.data_path)
        total_count = df.count()
        
        logger.info(f"✅ {total_count:,} transactions chargées")
        logger.info(f"📁 Partitions Spark: {df.rdd.getNumPartitions()}")
        
        return df
    
    def engineer_features(self, df):
        """Applique le feature engineering."""
        logger.info("🔧 Feature engineering...")
        
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
        
        logger.info(f"✅ {len(self.feature_columns)} features créées")
        return df
    
    def prepare_ml_data(self, df):
        """Prépare les données pour ML."""
        logger.info("📊 Préparation des données pour ML...")
        
        # Assembler les features
        assembler = VectorAssembler(inputCols=self.feature_columns, outputCol="features")
        
        # Renommer target
        df = df.withColumnRenamed("is_fraud", "label")
        
        # Appliquer l'assembleur
        df_ml = assembler.transform(df).select("features", "label")
        
        logger.info("✅ Données prêtes pour ML")
        return df_ml
    
    def train_test_split(self, df_ml):
        """Split train/test (80/20)."""
        logger.info("🔀 Split train/test (80/20)...")
        
        train_data, test_data = df_ml.randomSplit([0.8, 0.2], seed=42)
        
        # Cache pour accélérer
        train_data.cache()
        test_data.cache()
        
        train_count = train_data.count()
        test_count = test_data.count()
        
        logger.info(f"📊 Train: {train_count:,} transactions")
        logger.info(f"📊 Test: {test_count:,} transactions")
        
        return train_data, test_data
    
    def train_model(self, train_data, num_trees=100, max_depth=10):
        """Entraîne le Random Forest."""
        logger.info(f"🌲 Entraînement Random Forest (trees={num_trees}, depth={max_depth})...")
        
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            numTrees=num_trees,
            maxDepth=max_depth,
            maxBins=32,
            seed=42,
            featureSubsetStrategy="auto"
        )
        
        logger.info("⏳ Entraînement en cours (peut prendre 5-15 min)...")
        model = rf.fit(train_data)
        
        logger.info("✅ Entraînement terminé !")
        return model
    
    def evaluate_model(self, model, test_data):
        """Évalue le modèle."""
        logger.info("📊 Évaluation du modèle...")
        
        predictions = model.transform(test_data)
        
        # Métriques
        evaluator_auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
        evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        evaluator_prec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
        evaluator_rec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
        evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")
        
        metrics = {
            'auc': evaluator_auc.evaluate(predictions),
            'accuracy': evaluator_acc.evaluate(predictions),
            'precision': evaluator_prec.evaluate(predictions),
            'recall': evaluator_rec.evaluate(predictions),
            'f1_score': evaluator_f1.evaluate(predictions)
        }
        
        logger.info("="*60)
        logger.info("📊 MÉTRIQUES DE PERFORMANCE")
        logger.info("="*60)
        logger.info(f"🎯 Accuracy:  {metrics['accuracy']*100:.2f}%")
        logger.info(f"🎯 Precision: {metrics['precision']*100:.2f}%")
        logger.info(f"🎯 Recall:    {metrics['recall']*100:.2f}%")
        logger.info(f"🎯 F1-Score:  {metrics['f1_score']*100:.2f}%")
        logger.info(f"🎯 AUC-ROC:   {metrics['auc']:.4f}")
        logger.info("="*60)
        
        return metrics
    
    def save_model(self, model, metrics, train_count, test_count):
        """Sauvegarde le modèle et les métadonnées."""
        model_dir = Path("./data/models")
        model_dir.mkdir(parents=True, exist_ok=True)
        
        # Sauvegarder le modèle
        model_path = model_dir / "random_forest_fraud_detector"
        model.write().overwrite().save(str(model_path))
        logger.info(f"✅ Modèle sauvegardé dans: {model_path}")
        
        # Feature importance
        feature_importance = list(zip(self.feature_columns, model.featureImportances.toArray()))
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        
        # Métadonnées
        metadata = {
            'model_type': 'RandomForestClassifier',
            'num_trees': model.getNumTrees(),
            'max_depth': model.getMaxDepth(),
            'training_date': datetime.now().isoformat(),
            'training_samples': train_count,
            'test_samples': test_count,
            'metrics': metrics,
            'features': self.feature_columns,
            'feature_importance': [{'feature': f, 'importance': float(i)} for f, i in feature_importance]
        }
        
        metadata_path = model_dir / "model_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"✅ Métadonnées sauvegardées dans: {metadata_path}")
        
        # Afficher feature importance
        logger.info("\n📊 Top 5 Features les plus importantes:")
        for i, (feature, importance) in enumerate(feature_importance[:5], 1):
            logger.info(f"  {i}. {feature}: {importance:.4f}")
    
    def run(self):
        """Exécute le pipeline complet."""
        logger.info("="*60)
        logger.info("🚀 ENTRAÎNEMENT DU MODÈLE DE DÉTECTION DE FRAUDES")
        logger.info("="*60)
        
        try:
            # 1. Charger les données
            df = self.load_data()
            
            # 2. Feature engineering
            df = self.engineer_features(df)
            
            # 3. Préparer pour ML
            df_ml = self.prepare_ml_data(df)
            
            # 4. Split
            train_data, test_data = self.train_test_split(df_ml)
            
            # 5. Entraîner
            model = self.train_model(train_data)
            
            # 6. Évaluer
            metrics = self.evaluate_model(model, test_data)
            
            # 7. Sauvegarder
            self.save_model(model, metrics, train_data.count(), test_data.count())
            
            logger.info("\n" + "="*60)
            logger.info("✅ ENTRAÎNEMENT TERMINÉ AVEC SUCCÈS !")
            logger.info("="*60)
            logger.info("\n🎯 Prochaine étape: Intégrer le modèle au streaming")
            logger.info("   python run_detector_ml.py")
            
            return model, metrics
            
        except Exception as e:
            logger.error(f"❌ Erreur: {e}", exc_info=True)
            raise
        
        finally:
            self.spark.stop()
            logger.info("🛑 Spark arrêté")


if __name__ == "__main__":
    trainer = FraudModelTrainer(data_path="./data/historical")
    model, metrics = trainer.run()