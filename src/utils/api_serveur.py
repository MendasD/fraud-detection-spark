"""
API Server pour exposer les données de détection de fraudes
Ce serveur tourne dans le conteneur detector et expose les données via REST API
"""

from flask import Flask, jsonify
from flask_cors import CORS
from pyspark.sql import SparkSession
import logging
import time
import threading
import sys
from pathlib import Path

app = Flask(__name__)
CORS(app)  # Permettre les requêtes cross-origin

# Ajuster le path pour les imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Session Spark partagée
spark = None

def init_spark():
    """Initialise la session Spark"""
    global spark
    try:
        spark = SparkSession.builder \
            .appName("FraudDetectionML") \
            .master("local[*]") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("***Spark session initialized")
    except Exception as e:
        logger.error(f"!!! Error initializing Spark: {e}")

@app.route('/health', methods=['GET'])
def health():
    """Endpoint de santé"""
    return jsonify({'status': 'healthy', 'timestamp': time.time()})

@app.route('/api/fraud-data', methods=['GET'])
def get_fraud_data():
    """
    Récupère les données de détection depuis la table Spark
    """
    global spark
    
    if spark is None:
        return jsonify({'error': 'Spark not initialized'}), 500
    
    try:
        # Lire depuis la table en mémoire
        df = spark.sql("SELECT * FROM fraud_detection_ml")
        
        # Convertir en pandas puis en dict
        pdf = df.toPandas()
        
        # Convertir les timestamps en string pour JSON
        if 'timestamp' in pdf.columns:
            pdf['timestamp'] = pdf['timestamp'].astype(str)
        
        # Retourner les données en JSON
        data = pdf.to_dict(orient='records')
        
        return jsonify({
            'status': 'success',
            'count': len(data),
            'data': data,
            'timestamp': time.time()
        })
        
    except Exception as e:
        logger.error(f"!!! Error fetching data: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'message': 'Table may not exist yet or no data available'
        }), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """
    Récupère les statistiques agrégées
    """
    global spark
    
    if spark is None:
        return jsonify({'error': 'Spark not initialized'}), 500
    
    try:
        df = spark.sql("SELECT * FROM fraud_detection_ml")
        
        # Calculer les stats
        total = df.count()
        frauds = df.filter("predicted_fraud = 1.0").count()
        avg_score = df.agg({"fraud_score": "avg"}).collect()[0][0]
        
        # Métriques ML
        tp = df.filter("is_fraud = 1 AND predicted_fraud = 1.0").count()
        fp = df.filter("is_fraud = 0 AND predicted_fraud = 1.0").count()
        tn = df.filter("is_fraud = 0 AND predicted_fraud = 0.0").count()
        fn = df.filter("is_fraud = 1 AND predicted_fraud = 0.0").count()
        
        accuracy = (tp + tn) / total if total > 0 else 0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        
        return jsonify({
            'status': 'success',
            'stats': {
                'total_transactions': total,
                'frauds_detected': frauds,
                'fraud_rate': frauds / total if total > 0 else 0,
                'avg_fraud_score': float(avg_score) if avg_score else 0,
                'accuracy': accuracy,
                'precision': precision,
                'recall': recall,
                'confusion_matrix': {
                    'tp': tp,
                    'fp': fp,
                    'tn': tn,
                    'fn': fn
                }
            },
            'timestamp': time.time()
        })
        
    except Exception as e:
        logger.error(f"Error calculating stats: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500
    
def set_spark_session(spark_session):
    global spark
    spark = spark_session

def run_api_server():
    """Lance le serveur API"""
    logger.info("Starting API server on port 5000")
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)

if __name__ == '__main__':
    init_spark()
    run_api_server()