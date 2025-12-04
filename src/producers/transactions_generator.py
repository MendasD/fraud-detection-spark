"""
Générateur de transactions bancaires réalistes.
Simule des comportements normaux ET frauduleux selon des patterns définis.
"""

import os
import time
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import numpy as np
from faker import Faker
from dotenv import load_dotenv

import sys
#sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.models.transactions import Transaction, MERCHANT_CATEGORIES, FRAUD_PATTERNS
from src.utils.kafka_utils import (
    create_kafka_producer, 
    create_kafka_topic, 
    send_message,
    flush_and_close_producer
)
from src.utils.logger import setup_logger

load_dotenv()
logger = setup_logger(__name__)
fake = Faker('fr_FR')  # Données françaises


class TransactionGenerator:
    """
    Génère des transactions bancaires réalistes avec un mix de transactions
    normales et frauduleuses.
    """
    
    def __init__(self, num_users: int = 1000, fraud_rate: float = None):
        """
        Args:
            num_users: Nombre d'utilisateurs à simuler
            fraud_rate: Taux de fraude (0.0 à 1.0), ou utilise .env
        """
        self.num_users = num_users
        self.fraud_rate = fraud_rate or float(os.getenv('FRAUD_RATE', 0.02))
        
        # Initialiser les utilisateurs avec leurs profils
        self.users = self._create_user_profiles()
        
        # Historique des transactions par utilisateur (pour features temporelles)
        self.user_history: Dict[str, List[Transaction]] = {
            user_id: [] for user_id in self.users.keys()
        }
        
        logger.info(
            f"Générateur initialisé: {num_users} utilisateurs, "
            f"{self.fraud_rate*100:.1f}% de fraudes"
        )
    
    def _create_user_profiles(self) -> Dict[str, Dict]:
        """
        Crée des profils utilisateurs avec comportements typiques.
        
        Returns:
            Dictionnaire user_id -> profil
        """
        logger.info("Création des profils utilisateurs...")
        
        users = {}
        for i in range(self.num_users):
            user_id = f"USER_{i:06d}"
            
            # Localisation principale (domicile)
            home_lat = fake.latitude()
            home_lon = fake.longitude()
            
            # Catégories préférées (chaque user a ses habitudes)
            preferred_categories = random.sample(
                list(MERCHANT_CATEGORIES.keys()),
                k=random.randint(3, 6)
            )
            
            # Budget moyen et device habituel
            avg_transaction = random.uniform(30, 150)
            main_device = f"DEVICE_{uuid.uuid4().hex[:8]}"
            
            users[user_id] = {
                'home_location': (home_lat, home_lon),
                'preferred_categories': preferred_categories,
                'avg_transaction': avg_transaction,
                'std_transaction': avg_transaction * 0.3,
                'main_device': main_device,
                'card_last_4': str(random.randint(1000, 9999)),
                'typical_hours': list(range(8, 22)),  # Actif 8h-22h généralement
            }
        
        logger.info(f" {len(users)} profils créés")
        return users
    
    def _generate_normal_transaction(self, user_id: str) -> Transaction:
        """
        Génère une transaction normale basée sur le profil utilisateur.
        
        Args:
            user_id: ID de l'utilisateur
            
        Returns:
            Transaction normale
        """
        profile = self.users[user_id]
        
        # Choisir une catégorie parmi les préférences
        category = random.choice(profile['preferred_categories'])
        cat_info = MERCHANT_CATEGORIES[category]
        
        # Montant basé sur la catégorie et le profil
        amount = max(
            5.0,  # Minimum 5€
            np.random.normal(cat_info['avg_amount'], cat_info['std'])
        )
        
        # Location proche du domicile (dans un rayon de ~20km)
        home_lat, home_lon = profile['home_location']
        location_lat = float(home_lat) + random.uniform(-0.2, 0.2)
        location_lon = float(home_lon) + random.uniform(-0.2, 0.2)
        
        # Heure dans la plage habituelle
        current_hour = datetime.now().hour
        is_typical_hour = current_hour in profile['typical_hours']
        
        # Online/offline
        is_online = category == 'online_shopping' or random.random() < 0.2
        
        return Transaction(
            transaction_id=f"TRX_{uuid.uuid4().hex[:12].upper()}",
            user_id=user_id,
            timestamp=datetime.now().isoformat(),
            amount=round(amount, 2),
            merchant_id=f"MERCHANT_{random.randint(1000, 9999)}",
            merchant_category=category,
            location_lat=round(location_lat, 6),
            location_lon=round(location_lon, 6),
            device_id=profile['main_device'],
            is_online=is_online,
            is_fraud=0,
            card_last_4=profile['card_last_4'],
            cvv_provided=True
        )
    
    def _generate_fraudulent_transaction(self, user_id: str) -> Transaction:
        """
        Génère une transaction frauduleuse avec un pattern spécifique.
        
        Args:
            user_id: ID de l'utilisateur (victime)
            
        Returns:
            Transaction frauduleuse
        """
        profile = self.users[user_id]
        
        # Choisir un pattern de fraude aléatoire
        fraud_type = random.choice(list(FRAUD_PATTERNS.keys()))
        
        # Base: transaction normale modifiée
        category = random.choice(list(MERCHANT_CATEGORIES.keys()))
        cat_info = MERCHANT_CATEGORIES[category]
        
        if fraud_type == 'high_amount':
            # Montant anormalement élevé
            amount = cat_info['avg_amount'] * FRAUD_PATTERNS['high_amount']['multiplier']
            location_lat, location_lon = profile['home_location']
            device_id = profile['main_device']
            
        elif fraud_type == 'geographic_impossible':
            # Transaction dans un lieu très éloigné
            amount = cat_info['avg_amount'] * random.uniform(2, 5)
            # Location aléatoire loin du domicile
            location_lat = fake.latitude()
            location_lon = fake.longitude()
            device_id = profile['main_device']
            
        elif fraud_type == 'new_device':
            # Nouveau device + montant élevé
            amount = max(
                FRAUD_PATTERNS['new_device']['amount_threshold'],
                cat_info['avg_amount'] * random.uniform(3, 6)
            )
            location_lat, location_lon = profile['home_location']
            device_id = f"DEVICE_{uuid.uuid4().hex[:8]}"  # Nouveau device!
            
        else:  # unusual_time ou rapid_succession
            amount = cat_info['avg_amount'] * random.uniform(1.5, 4)
            location_lat, location_lon = profile['home_location']
            device_id = profile['main_device']
        
        return Transaction(
            transaction_id=f"TRX_{uuid.uuid4().hex[:12].upper()}",
            user_id=user_id,
            timestamp=datetime.now().isoformat(),
            amount=round(amount, 2),
            merchant_id=f"MERCHANT_{random.randint(1000, 9999)}",
            merchant_category=category,
            location_lat=round(location_lat, 6),
            location_lon=round(location_lon, 6),
            device_id=device_id,
            is_online=random.random() < 0.5,
            is_fraud=1,  # FRAUDE
            card_last_4=profile['card_last_4'],
            cvv_provided=random.random() < 0.3  # Souvent pas de CVV en fraude
        )
    
    def generate_transaction(self) -> Transaction:
        """
        Génère une transaction (normale ou frauduleuse selon fraud_rate).
        
        Returns:
            Transaction générée
        """
        # Choisir un utilisateur aléatoire
        user_id = random.choice(list(self.users.keys()))
        
        # Décider si fraude ou non
        is_fraud = random.random() < self.fraud_rate
        
        if is_fraud:
            transaction = self._generate_fraudulent_transaction(user_id)
            logger.debug(f" Fraude générée: {transaction.transaction_id}")
        else:
            transaction = self._generate_normal_transaction(user_id)
            logger.debug(f" Transaction normale: {transaction.transaction_id}")
        
        # Ajouter à l'historique
        self.user_history[user_id].append(transaction)
        
        # Garder seulement les 100 dernières transactions par user
        if len(self.user_history[user_id]) > 100:
            self.user_history[user_id] = self.user_history[user_id][-100:]
        
        return transaction
    
    def run(self, duration_seconds: int = None, transactions_per_second: int = None):
        """
        Lance la génération continue de transactions vers Kafka.
        
        Args:
            duration_seconds: Durée d'exécution (None = infini)
            transactions_per_second: Débit (ou utilise .env)
        """
        tps = transactions_per_second or int(os.getenv('TRANSACTION_RATE', 1000))
        delay = 1.0 / tps  # Délai entre transactions
        
        topic = os.getenv('KAFKA_TOPIC', 'transactions')
        
        # Créer le topic s'il n'existe pas
        create_kafka_topic()
        
        # Créer le producteur Kafka
        producer = create_kafka_producer()
        
        logger.info(f" Démarrage: {tps} transactions/sec vers topic '{topic}'")
        
        start_time = time.time()
        transaction_count = 0
        fraud_count = 0
        
        try:
            while True:
                # Vérifier la durée si spécifiée
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    break
                
                # Générer et envoyer une transaction
                transaction = self.generate_transaction()
                
                # Convertir en dict pour Kafka
                transaction_dict = {
                    'transaction_id': transaction.transaction_id,
                    'user_id': transaction.user_id,
                    'timestamp': transaction.timestamp,
                    'amount': float(transaction.amount),
                    'merchant_id': transaction.merchant_id,
                    'merchant_category': transaction.merchant_category,
                    'location_lat': float(transaction.location_lat),
                    'location_lon': float(transaction.location_lon),
                    'device_id': transaction.device_id,
                    'is_online': bool(transaction.is_online),
                    'is_fraud': int(transaction.is_fraud),
                    'card_last_4': transaction.card_last_4,
                    'cvv_provided': transaction.cvv_provided,
                }
                
                # Envoyer à Kafka (key = user_id pour partitionnement)
                send_message(producer, topic, transaction_dict, key=transaction.user_id)
                
                transaction_count += 1
                if transaction.is_fraud:
                    fraud_count += 1
                
                # Stats toutes les 1000 transactions
                if transaction_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    actual_tps = transaction_count / elapsed
                    fraud_pct = (fraud_count / transaction_count) * 100
                    
                    logger.info(
                        f" Stats: {transaction_count} transactions, "
                        f"{actual_tps:.0f} TPS réel, "
                        f"{fraud_pct:.1f}% fraudes"
                    )
                
                # Respecter le débit
                time.sleep(delay)
                
        except KeyboardInterrupt:
            logger.info("\n Arrêt demandé par l'utilisateur")
        
        except Exception as e:
            logger.error(f" Erreur: {e}", exc_info=True)
        
        finally:
            # Statistiques finales
            elapsed = time.time() - start_time
            logger.info(
                f"\n ### Résumé ###:\n"
                f"  -- Durée: {elapsed:.1f}s\n"
                f"  -- Transactions: {transaction_count}\n"
                f"  -- Fraudes: {fraud_count} ({(fraud_count/transaction_count*100):.1f}%)\n" if transaction_count > 0 else f"  -- Fraudes: 0.0%\n"
                f"  -- Débit moyen: {transaction_count/elapsed:.0f} TPS"
            )
            
            # Fermer proprement Kafka
            flush_and_close_producer(producer)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Générateur de transactions bancaires')
    parser.add_argument('--users', type=int, default=1000, help='Nombre d\'utilisateurs')
    parser.add_argument('--duration', type=int, help='Durée en secondes (défaut: infini)')
    parser.add_argument('--tps', type=int, help='Transactions par seconde')
    parser.add_argument('--fraud-rate', type=float, help='Taux de fraude (0.0-1.0)')
    
    args = parser.parse_args()
    
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