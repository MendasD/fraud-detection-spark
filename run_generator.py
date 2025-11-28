"""
Script de lancement du générateur de transactions.
"""

import sys
import argparse
from src.producers.transactions_generator import TransactionGenerator

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Générateur de transactions bancaires')
    parser.add_argument('--users', type=int, default=1000, help='Nombre d\'utilisateurs')
    parser.add_argument('--duration', type=int, help='Durée en secondes (défaut: infini)')
    parser.add_argument('--tps', type=int, help='Transactions par seconde')
    parser.add_argument('--fraud-rate', type=float, help='Taux de fraude (0.0-1.0)')
    
    args = parser.parse_args()
    
    print(" Démarrage du générateur de transactions...\n")
    
    # Créer le générateur
    generator = TransactionGenerator(
        num_users=args.users,
        fraud_rate=args.fraud_rate
    )
    
    # Lancer la génération
    generator.run(
        duration_seconds=args.duration,
        transactions_per_second=args.tps
    )