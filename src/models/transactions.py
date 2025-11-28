"""
Modèle de données pour les transactions bancaires.
Définit la structure exacte qu'on enverra à Kafka et que Spark traitera.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional
import json


@dataclass
class Transaction:
    """
    Représente une transaction bancaire.
    
    Attributes:
        transaction_id: Identifiant unique de la transaction
        user_id: ID de l'utilisateur (carte bancaire)
        timestamp: Moment de la transaction
        amount: Montant en euros
        merchant_id: ID du commerçant
        merchant_category: Catégorie (restaurant, supermarché, etc.)
        location_lat: Latitude de la transaction
        location_lon: Longitude de la transaction
        device_id: Identifiant de l'appareil utilisé
        is_online: Transaction en ligne ou physique
        is_fraud: Label (0=normal, 1=fraude) - pour l'entraînement
    """
    transaction_id: str
    user_id: str
    timestamp: str  # ISO format
    amount: float
    merchant_id: str
    merchant_category: str
    location_lat: float
    location_lon: float
    device_id: str
    is_online: bool
    is_fraud: int  # 0 ou 1
    
    # Champs optionnels pour features avancées
    card_last_4: Optional[str] = None
    cvv_provided: Optional[bool] = None
    
    def to_json(self) -> str:
        """Convertit la transaction en JSON pour Kafka."""
        return json.dumps(asdict(self))
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Transaction':
        """Reconstruit une transaction depuis JSON."""
        data = json.loads(json_str)
        return cls(**data)
    
    def __str__(self) -> str:
        """Représentation lisible de la transaction."""
        fraud_label = "[FRAUDE]" if self.is_fraud else "[Normal]"
        return (f"Transaction {self.transaction_id}: "
                f"{self.amount:.2f}€ - {self.merchant_category} - {fraud_label}")


# Catégories de marchands avec patterns typiques
MERCHANT_CATEGORIES = {
    'supermarket': {'avg_amount': 45, 'std': 25, 'frequency': 'high'},
    'restaurant': {'avg_amount': 35, 'std': 20, 'frequency': 'medium'},
    'gas_station': {'avg_amount': 60, 'std': 15, 'frequency': 'medium'},
    'pharmacy': {'avg_amount': 25, 'std': 15, 'frequency': 'low'},
    'electronics': {'avg_amount': 350, 'std': 200, 'frequency': 'low'},
    'clothing': {'avg_amount': 80, 'std': 40, 'frequency': 'low'},
    'hotel': {'avg_amount': 150, 'std': 80, 'frequency': 'very_low'},
    'airline': {'avg_amount': 250, 'std': 150, 'frequency': 'very_low'},
    'online_shopping': {'avg_amount': 60, 'std': 40, 'frequency': 'medium'},
    'entertainment': {'avg_amount': 40, 'std': 20, 'frequency': 'low'},
}


# Patterns de fraude à simuler
FRAUD_PATTERNS = {
    'high_amount': {
        'description': 'Montant anormalement élevé',
        'multiplier': 10,  # 10x le montant habituel
    },
    'rapid_succession': {
        'description': 'Plusieurs transactions rapides',
        'count': 5,
        'timeframe_seconds': 60,
    },
    'geographic_impossible': {
        'description': 'Transactions dans des lieux impossibles (trop éloignés)',
        'min_distance_km': 500,
        'timeframe_minutes': 30,
    },
    'unusual_time': {
        'description': 'Transaction à une heure inhabituelle',
        'hours': [2, 3, 4],  # 2h-4h du matin
    },
    'new_device': {
        'description': 'Nouveau device + montant élevé',
        'amount_threshold': 200,
    },
}


if __name__ == "__main__":
    # Test du modèle
    from datetime import datetime
    
    # Créer une transaction de test
    test_transaction = Transaction(
        transaction_id="TRX_001",
        user_id="USER_123",
        timestamp=datetime.now().isoformat(),
        amount=45.99,
        merchant_id="MERCHANT_456",
        merchant_category="supermarket",
        location_lat=48.8566,
        location_lon=2.3522,
        device_id="DEVICE_789",
        is_online=False,
        is_fraud=0,
        card_last_4="1234"
    )
    
    print("Transaction créée:")
    print(test_transaction)
    print("\nJSON:")
    print(test_transaction.to_json())
    print("\nReconstruction depuis JSON:")
    reconstructed = Transaction.from_json(test_transaction.to_json())
    print(reconstructed)