"""
Script simplifié pour lancer le détecteur ML sans spark-submit
Résout les problèmes de chemin Python et SPARK_HOME
"""

import os
import sys
from dotenv import load_dotenv
import threading
import time
import sys
from pathlib import Path
from waitress import serve

# Ajuster le path pour les imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.utils.logger import setup_logger

# Charger les variables d'environnement
load_dotenv()

logger = setup_logger(__name__)

python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = os.getenv('PYSPARK_PYTHON', python_path)
os.environ["PYSPARK_DRIVER_PYTHON"] = os.getenv('PYSPARK_PYTHON', python_path)
java_home = os.getenv('JAVA_HOME') # Java 21 compatible pour spark 3.5.x
if java_home:
    os.environ["JAVA_HOME"] = java_home
else:
    pass # on laisse java du conteneur
#os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-21.0.6.7-hotspot" # Java 17 compatible pour spark 3.5.x

# Configurer HADOOP_HOME pour Windows si défini dans .env
hadoop_home = os.getenv('HADOOP_HOME')
if hadoop_home and os.path.exists(hadoop_home):
    os.environ['HADOOP_HOME'] = hadoop_home
    bin_path = os.path.join(hadoop_home, 'bin')
    if bin_path not in os.environ.get('PATH', ''):
        os.environ['PATH'] = f"{bin_path};{os.environ.get('PATH', '')}"
    #logger.info(f"HADOOP_HOME configuré: {hadoop_home}")

# Désactiver le reloader de Werkzeug pour éviter les conflits
os.environ['WERKZEUG_RUN_MAIN'] = 'true'

print("="*60)
print("*** LANCEMENT DU DETECTEUR ML ***")
print("="*60)
print(f"Python: {python_path}")
print(f"PYSPARK_PYTHON: {os.environ['PYSPARK_PYTHON']}")
print(f"PYSPARK_DRIVER_PYTHON: {os.environ['PYSPARK_DRIVER_PYTHON']}")
print("="*60)
print()

# Importer et lancer le détecteur
from src.streaming.ml_fraud_detector import MlFraudDetector
from src.utils.spark_utils import (
    create_spark_session,
)


def run_api_server():
    """Lance le serveur API dans un thread séparé"""
    #from src.utils.api_serveur import run_api_server as start_api
    from src.utils.api_serveur import app, set_spark_session
    logger.info("Starting session Spark créée pour l'API")
    spark_session = create_spark_session()
    logger.info("Session Spark créée pour l'API")

    set_spark_session(spark_session)
    print("🌐 Démarrage du serveur API sur port 5000...")
    serve(app, host="0.0.0.0", port=5000, threads=1)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Détecteur de fraudes ML')
    parser.add_argument('--mode', choices=['console', 'parquet', 'memory', 'all'],
                       default='memory', help='Mode de sortie')
    parser.add_argument('--trigger', default='5 seconds',
                       help='Intervalle de traitement')
    
    args = parser.parse_args()
    
    print(f"* Mode: {args.mode}")
    print(f"*  Trigger: {args.trigger}")
    print()

    # Lancer l'API dans un thread
    api_thread = threading.Thread(target=run_api_server, daemon=True)
    api_thread.start()

    print("=== Attente 5 secondes pour l'API... ===")
    logger.info("=== Attente 5 secondes pour l'API... ===")
    time.sleep(5)
    
    try:
        detector = MlFraudDetector()
        detector.run(mode=args.mode, trigger_interval=args.trigger)
    except KeyboardInterrupt:
        print("\n\n!!* Arrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"\n\n!!! Erreur: {e}")
        import traceback
        traceback.print_exc()