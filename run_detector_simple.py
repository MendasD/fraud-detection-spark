"""
Script simplifié pour lancer le détecteur ML sans spark-submit
Résout les problèmes de chemin Python et SPARK_HOME
"""

import os
import sys
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

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
    
    try:
        detector = MlFraudDetector()
        detector.run(mode=args.mode, trigger_interval=args.trigger)
    except KeyboardInterrupt:
        print("\n\n!!* Arrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"\n\n!!! Erreur: {e}")
        import traceback
        traceback.print_exc()