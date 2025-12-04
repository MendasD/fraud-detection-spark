"""
Générateur de données historiques pour l'entraînement ML.
Génère 10-15 millions de transactions sur 1 an avec patterns réalistes.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import random
import uuid
import numpy as np
from faker import Faker
import pandas as pd
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.models.transactions import MERCHANT_CATEGORIES
from src.utils.logger import setup_logger

logger = setup_logger(__name__)
fake = Faker('fr_FR')


class UserProfile:
    """Profil utilisateur avec comportements cohérents dans le temps."""
    
    def __init__(self, user_id: str):
        self.user_id = user_id
        
        # Localisation stable
        self.home_lat = float(fake.latitude())
        self.home_lon = float(fake.longitude())
        
        # Profil de dépenses (stable)
        self.avg_monthly_spend = random.uniform(300, 2000)
        self.daily_transaction_prob = random.uniform(0.6, 0.95)  # Prob de faire au moins 1 txn/jour
        
        # Catégories préférées (3-6 catégories)
        all_categories = list(MERCHANT_CATEGORIES.keys())
        self.preferred_categories = random.sample(all_categories, k=random.randint(3, 6))
        
        # Patterns temporels (heures d'activité)
        self.active_hours = self._generate_active_hours()
        
        # Devices (1-2 devices stables)
        self.devices = [f"DEVICE_{uuid.uuid4().hex[:8]}" for _ in range(random.randint(1, 2))]
        self.main_device = self.devices[0]
        
        # Carte bancaire
        self.card_last_4 = str(random.randint(1000, 9999))
        
        # Historique de montants (pour détecter anomalies)
        self.transaction_amounts = []
        
    def _generate_active_hours(self):
        """Génère les heures d'activité typiques."""
        # La plupart des gens sont actifs 8h-23h
        base_hours = list(range(8, 23))
        # Quelques personnes actives la nuit
        if random.random() < 0.1:
            base_hours.extend([0, 1, 2, 23])
        return base_hours
    
    def get_typical_amount(self, category: str) -> float:
        """Retourne un montant typique pour cette catégorie."""
        cat_info = MERCHANT_CATEGORIES[category]
        return cat_info['avg_amount']
    
    def update_history(self, amount: float):
        """Met à jour l'historique des montants."""
        self.transaction_amounts.append(amount)
        if len(self.transaction_amounts) > 100:
            self.transaction_amounts = self.transaction_amounts[-100:]


class HistoricalDataGenerator:
    """Générateur de données historiques massives."""
    
    def __init__(
        self,
        num_users: int = 5000,
        start_date: datetime = None,
        end_date: datetime = None,
        fraud_rate: float = 0.02
    ):
        self.num_users = num_users
        self.start_date = start_date or (datetime.now() - timedelta(days=365))
        self.end_date = end_date or datetime.now()
        self.fraud_rate = fraud_rate
        
        logger.info(f"Initialisation du générateur:")
        logger.info(f"  - Utilisateurs: {num_users}")
        logger.info(f"  - Période: {self.start_date.date()} à {self.end_date.date()}")
        logger.info(f"  - Taux de fraude: {fraud_rate*100:.1f}%")
        
        # Créer les profils utilisateurs
        self.users = self._create_user_profiles()
        
    def _create_user_profiles(self):
        """Crée tous les profils utilisateurs."""
        logger.info("Création des profils utilisateurs...")
        users = {}
        for i in range(self.num_users):
            user_id = f"USER_{i:06d}"
            users[user_id] = UserProfile(user_id)
        logger.info(f"### {len(users)} profils créés ###")
        return users
    
    def _should_transact(self, user: UserProfile, timestamp: datetime) -> bool:
        """Détermine si l'utilisateur fait une transaction à ce moment."""
        # Probabilité de base
        if random.random() > user.daily_transaction_prob:
            return False
        
        # Ajustement par heure
        if timestamp.hour in user.active_hours:
            return random.random() < 0.15  # 15% de chance par heure active
        else:
            return random.random() < 0.02  # 2% de chance hors heures actives
        
    def _generate_normal_transaction(self, user: UserProfile, timestamp: datetime) -> dict:
        """Génère une transaction normale cohérente."""
        # Choisir une catégorie préférée
        category = random.choice(user.preferred_categories)
        cat_info = MERCHANT_CATEGORIES[category]
        
        # Montant cohérent avec la catégorie et l'utilisateur
        base_amount = cat_info['avg_amount']
        # Variation +/- 40% autour de la moyenne
        amount = max(5.0, base_amount * random.uniform(0.6, 1.4))
        
        # Localisation cohérente (dans un rayon de 20km du domicile)
        location_lat = user.home_lat + random.uniform(-0.2, 0.2)
        location_lon = user.home_lon + random.uniform(-0.2, 0.2)
        
        # Device habituel (95% du temps)
        device_id = user.main_device if random.random() < 0.95 else random.choice(user.devices)
        
        # Online selon la catégorie
        is_online = (category == 'online_shopping') or (random.random() < 0.2)
        
        return {
            'transaction_id': f"TRX_{uuid.uuid4().hex[:12].upper()}",
            'user_id': user.user_id,
            'timestamp': timestamp.isoformat(),
            'amount': round(amount, 2),
            'merchant_id': f"MERCHANT_{random.randint(1000, 9999)}",
            'merchant_category': category,
            'location_lat': round(location_lat, 6),
            'location_lon': round(location_lon, 6),
            'device_id': device_id,
            'is_online': is_online,
            'is_fraud': 0,
            'card_last_4': user.card_last_4,
            'cvv_provided': True
        }
    
    def _generate_fraudulent_transaction(self, user: UserProfile, timestamp: datetime) -> dict:
        """Génère une transaction frauduleuse avec un pattern réaliste."""
        # Choisir un type de fraude
        fraud_types = ['high_amount', 'rapid_succession', 'geographic', 'new_device', 'unusual_time']
        fraud_type = random.choice(fraud_types)
        
        category = random.choice(list(MERCHANT_CATEGORIES.keys()))
        cat_info = MERCHANT_CATEGORIES[category]
        
        if fraud_type == 'high_amount':
            # Montant anormalement élevé (5-15x la normale)
            avg_user_amount = np.mean(user.transaction_amounts) if user.transaction_amounts else cat_info['avg_amount']
            amount = avg_user_amount * random.uniform(5, 15)
            location_lat = user.home_lat + random.uniform(-0.2, 0.2)
            location_lon = user.home_lon + random.uniform(-0.2, 0.2)
            device_id = user.main_device
            
        elif fraud_type == 'geographic':
            # Transaction dans un lieu très éloigné
            amount = cat_info['avg_amount'] * random.uniform(2, 6)
            location_lat = float(fake.latitude())  # Lieu aléatoire loin
            location_lon = float(fake.longitude())
            device_id = user.main_device
            
        elif fraud_type == 'new_device':
            # Nouveau device + montant élevé
            amount = cat_info['avg_amount'] * random.uniform(3, 8)
            location_lat = user.home_lat + random.uniform(-0.2, 0.2)
            location_lon = user.home_lon + random.uniform(-0.2, 0.2)
            device_id = f"DEVICE_{uuid.uuid4().hex[:8]}"  # Nouveau device!
            
        elif fraud_type == 'unusual_time':
            # Transaction à une heure inhabituelle + montant élevé
            amount = cat_info['avg_amount'] * random.uniform(3, 7)
            location_lat = user.home_lat + random.uniform(-0.2, 0.2)
            location_lon = user.home_lon + random.uniform(-0.2, 0.2)
            device_id = user.main_device
            # Force l'heure à 2h-5h du matin
            timestamp = timestamp.replace(hour=random.randint(2, 5))
            
        else:  # rapid_succession
            # Montant élevé (sera en série)
            amount = cat_info['avg_amount'] * random.uniform(2, 5)
            location_lat = user.home_lat + random.uniform(-0.2, 0.2)
            location_lon = user.home_lon + random.uniform(-0.2, 0.2)
            device_id = user.main_device
        
        return {
            'transaction_id': f"TRX_{uuid.uuid4().hex[:12].upper()}",
            'user_id': user.user_id,
            'timestamp': timestamp.isoformat(),
            'amount': round(amount, 2),
            'merchant_id': f"MERCHANT_{random.randint(1000, 9999)}",
            'merchant_category': category,
            'location_lat': round(location_lat, 6),
            'location_lon': round(location_lon, 6),
            'device_id': device_id,
            'is_online': random.random() < 0.5,
            'is_fraud': 1,
            'card_last_4': user.card_last_4,
            'cvv_provided': random.random() < 0.3  # Moins souvent de CVV en fraude
        }
    
    def generate(self, output_path: str = "../../data/historical"):
        """Génère toutes les transactions et les sauvegarde en Parquet."""
        output_path = Path(output_path)
        output_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f" Début de la génération")
        logger.info(f" Sortie: {output_path}")
        
        total_days = (self.end_date - self.start_date).days
        transactions_batch = []
        total_transactions = 0
        total_frauds = 0
        
        # Générer jour par jour
        current_month = None

        logger.info(f" Première date courante: {self.start_date + timedelta(days=0)}")
        
        for day_offset in tqdm(range(total_days), desc="Génération"):
            current_date = self.start_date + timedelta(days=day_offset)
            logger.info(f" Date courante: {current_date}")
            
            # Sauvegarder le mois précédent si on change de mois
            if current_month is not None and current_date.month != current_month :
                if transactions_batch:
                    df = pd.DataFrame(transactions_batch)
                    
                    # Utiliser la date du mois précédent pour le nom
                    prev_date = current_date - timedelta(days=1)
                    year_month_path = output_path / f"year={prev_date.year}" / f"month={prev_date.month:02d}"
                    year_month_path.mkdir(parents=True, exist_ok=True)
                    
                    parquet_file = year_month_path / f"data_{prev_date.strftime('%Y%m')}.parquet"
                    df.to_parquet(parquet_file, index=False, compression='snappy')
                    
                    logger.info(f"------- Sauvegardé: {len(transactions_batch):,} transactions pour {prev_date.strftime('%Y-%m')}")
                    transactions_batch = []
            
            current_month = current_date.month
            
            # Pour chaque utilisateur
            for user in self.users.values():
                # Nombre de transactions ce jour (0-5)
                num_transactions = 0
                if random.random() < user.daily_transaction_prob:
                    num_transactions = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 20, 7, 3])[0]
                
                for _ in range(num_transactions):
                    # Heure aléatoire dans la journée
                    hour = random.choice(user.active_hours)
                    minute = random.randint(0, 59)
                    second = random.randint(0, 59)
                    timestamp = current_date.replace(hour=hour, minute=minute, second=second)
                    
                    # Décider si fraude
                    is_fraud = random.random() < self.fraud_rate
                    
                    if is_fraud:
                        transaction = self._generate_fraudulent_transaction(user, timestamp)
                        total_frauds += 1
                    else:
                        transaction = self._generate_normal_transaction(user, timestamp)
                    
                    # Mettre à jour l'historique utilisateur
                    user.update_history(transaction['amount'])
                    
                    transactions_batch.append(transaction)
                    total_transactions += 1
        
        # Sauvegarder le dernier mois
        if transactions_batch:
            df = pd.DataFrame(transactions_batch)
            
            year_month_path = output_path / f"year={current_date.year}" / f"month={current_date.month:02d}"
            year_month_path.mkdir(parents=True, exist_ok=True)
            
            parquet_file = year_month_path / f"data_{current_date.strftime('%Y%m')}.parquet"
            df.to_parquet(parquet_file, index=False, compression='snappy')
            
            logger.info(f"-------- Sauvegardé: {len(transactions_batch):,} transactions pour {current_date.strftime('%Y-%m')}")
        
        # Statistiques finales
        fraud_rate_actual = (total_frauds / total_transactions) * 100 if total_transactions > 0 else 0
        
        logger.info("\n" + "="*60)
        logger.info("*** GÉNÉRATION TERMINÉE ***")
        logger.info("=\""*60)
        logger.info(f"* Total transactions: {total_transactions:,}")
        logger.info(f"* Total fraudes: {total_frauds:,} ({fraud_rate_actual:.2f}%)")
        logger.info(f"* Utilisateurs: {self.num_users:,}")
        logger.info(f"* Période: {self.start_date.date()} à {self.end_date.date()}")
        logger.info(f"* Données sauvegardées dans: {output_path}")
        logger.info("=\""*60)
        
        return {
            'total_transactions': total_transactions,
            'total_frauds': total_frauds,
            'fraud_rate': fraud_rate_actual,
            'output_path': str(output_path)
        }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Génère des données historiques massives')
    parser.add_argument('--users', type=int, default=5000, help='Nombre d\'utilisateurs')
    parser.add_argument('--days', type=int, default=365, help='Nombre de jours à générer')
    parser.add_argument('--fraud-rate', type=float, default=0.02, help='Taux de fraude')
    parser.add_argument('--output', type=str, default='./data/historical', help='Dossier de sortie')
    
    args = parser.parse_args()
    
    # Calculer les dates
    end_date = datetime.now()
    start_date = end_date - timedelta(days=args.days)
    
    # Créer le générateur
    generator = HistoricalDataGenerator(
        num_users=args.users,
        start_date=start_date,
        end_date=end_date,
        fraud_rate=args.fraud_rate
    )
    
    # Générer les données
    stats = generator.generate(output_path=args.output)
    
    print("\n Données prêtes pour l'entraînement ML !")
    print(f" Vous pouvez maintenant utiliser: data/historical/")