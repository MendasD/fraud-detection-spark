"""
Script rapide pour générer les données historiques.
Usage: python run_generate_data.py
"""

from datetime import datetime, timedelta
from src.data_generation.generate_historical import HistoricalDataGenerator

if __name__ == "__main__":
    print("="*60)
    print("!!***!! GÉNÉRATION DE DONNÉES HISTORIQUES MASSIVES !!***!!")
    print("="*60)
    
    # Configuration
    NUM_USERS = 5000      # 5000 utilisateurs
    NUM_DAYS = 365        # 1 an
    FRAUD_RATE = 0.02     # 2%
    
    # Dates
    end_date = datetime.now()
    start_date = end_date - timedelta(days=NUM_DAYS)
    
    print(f"\n### Configuration: ###")
    print(f"  * Utilisateurs: {NUM_USERS:,}")
    print(f"  * Période: {start_date.date()} à {end_date.date()}")
    print(f"  * Taux de fraude: {FRAUD_RATE*100}%")
    print(f"\n** Estimation: ~{NUM_USERS * NUM_DAYS * 2.5 / 1_000_000:.1f}M transactions")
    print(f"** Taille estimée: ~{NUM_USERS * NUM_DAYS * 2.5 * 0.5 / 1000:.1f} GB")
    
    input("\n  Appuyez sur Entrée pour démarrer la génération...")
    
    # Créer le générateur
    generator = HistoricalDataGenerator(
        num_users=NUM_USERS,
        start_date=start_date,
        end_date=end_date,
        fraud_rate=FRAUD_RATE
    )
    
    # Générer
    stats = generator.generate(output_path='./data/historical')
    
    print("\n" + "="*60)
    print(" GÉNÉRATION TERMINÉE !")
    print("="*60)
    print(f"\n### Statistiques: ###")
    print(f"  * Transactions: {stats['total_transactions']:,}")
    print(f"  * Fraudes: {stats['total_frauds']:,} ({stats['fraud_rate']:.2f}%)")
    print(f"\n Données sauvegardées dans: data/historical/")
    print(f"\n Prochaine étape:")
    print(f"  python run_train_model.py")
    print("="*60)