"""
Script de lancement du détecteur de fraudes.
À exécuter depuis la racine du projet.
"""

import sys
import argparse
from src.streaming.fraud_detector import FraudDetector
from src.streaming.ml_fraud_detector import MlFraudDetector

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Détecteur de fraudes en temps réel avec Spark')
    parser.add_argument(
        '--mode',
        choices=['console', 'parquet', 'memory', 'all'],
        default='console',
        help='Mode de sortie: console (debug), parquet (persistence), memory (dashboard), all (tous)'
    )
    parser.add_argument(
        '--trigger',
        default='5 seconds',
        help='Intervalle de traitement (ex: "5 seconds", "10 seconds", "1 minute")'
    )
    
    args = parser.parse_args()
    
    print(" ***** Démarrage du système de détection de fraudes...  ******\n")
    print(f" ** Mode: {args.mode}")
    print(f" **  Intervalle: {args.trigger}\n")
    
    # Créer et lancer le détecteur
    detector = MlFraudDetector()
    detector.run(mode=args.mode, trigger_interval=args.trigger)