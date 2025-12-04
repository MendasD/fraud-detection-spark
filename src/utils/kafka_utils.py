"""
Utilitaires pour interagir avec Kafka.
Encapsule la création de producteurs et consommateurs.
"""

import os
from typing import Optional
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from dotenv import load_dotenv
import json

from pathlib import Path
import sys

# Ajuster le path pour les imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.utils.logger import setup_logger

# Charger les variables d'environnement
load_dotenv()

logger = setup_logger(__name__)


def create_kafka_producer(bootstrap_servers: Optional[str] = None) -> KafkaProducer:
    """
    Crée un producteur Kafka configuré pour envoyer des messages JSON.
    
    Args:
        bootstrap_servers: Adresse du broker Kafka (ou utilise .env)
        
    Returns:
        KafkaProducer configuré
    """
    servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    logger.info(f"Création du producteur Kafka sur {servers}")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=servers,
            # Sérialisation automatique en JSON
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            api_version=(3, 7, 0),
            # Configuration pour performance
            acks='all',  # Attendre confirmation de tous les replicas
            compression_type='gzip',  # Compression pour réduire la bande passante
            batch_size=16384,  # Taille du batch (16KB)
            linger_ms=10,  # Attendre 10ms pour remplir le batch
            buffer_memory=33554432,  # 32MB de buffer
            max_request_size=1048576,  # 1MB max par requête
        )
        
        logger.info("Producteur Kafka créé avec succès")
        return producer
        
    except Exception as e:
        logger.error(f"Erreur lors de la création du producteur: {e}")
        raise


def create_kafka_topic(
    topic_name: Optional[str] = None,
    num_partitions: int = 6,
    replication_factor: int = 1,
    bootstrap_servers: Optional[str] = None
) -> bool:
    """
    Crée un topic Kafka s'il n'existe pas.
    
    Args:
        topic_name: Nom du topic (ou utilise .env)
        num_partitions: Nombre de partitions (pour parallélisme)
        replication_factor: Facteur de réplication
        bootstrap_servers: Adresse du broker
        
    Returns:
        True si créé ou existe déjà, False en cas d'erreur
    """
    topic = topic_name or os.getenv('KAFKA_TOPIC', 'transactions')
    servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    logger.info(f"Vérification/création du topic '{topic}'")
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=servers,
            client_id='fraud_detection_admin'
        )
        
        # Définir le topic
        topic_config = NewTopic(
            name=topic,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            # Configuration pour rétention et performance
            topic_configs={
                'retention.ms': '604800000',  # 7 jours
                'segment.bytes': '1073741824',  # 1GB par segment
                'compression.type': 'gzip',
            }
        )
        
        # Créer le topic
        admin_client.create_topics(new_topics=[topic_config], validate_only=False)
        logger.info(f"Topic '{topic}' créé avec {num_partitions} partitions")
        return True
        
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{topic}' existe déjà")
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors de la création du topic: {e}")
        return False

def send_message(
    producer: KafkaProducer,
    topic: str,
    message: dict,
    key: Optional[str] = None
) -> bool:
    """
    Envoie un message à Kafka de manière sécurisée.
    
    Args:
        producer: Instance du producteur Kafka
        topic: Nom du topic
        message: Dictionnaire à envoyer (sera converti en JSON)
        key: Clé optionnelle pour le partitionnement
        
    Returns:
        True si envoyé avec succès, False sinon
    """
    try:
        # Envoi asynchrone
        future = producer.send(
            topic=topic,
            value=message,
            key=key
        )
        
        # Attendre la confirmation (avec timeout)
        record_metadata = future.get(timeout=10)
        
        logger.debug(
            f"Message envoyé - Topic: {record_metadata.topic}, "
            f"Partition: {record_metadata.partition}, "
            f"Offset: {record_metadata.offset}"
        )
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors de l'envoi du message: {e}")
        return False


def flush_and_close_producer(producer: KafkaProducer):
    """
    Flush les messages en attente et ferme proprement le producteur.
    
    Args:
        producer: Instance du producteur à fermer
    """
    try:
        logger.info("Flush des messages en attente...")
        producer.flush()
        logger.info("Fermeture du producteur Kafka")
        producer.close()
    except Exception as e:
        logger.error(f"Erreur lors de la fermeture du producteur: {e}")


if __name__ == "__main__":
    # Test des utilitaires
    print("Test des utilitaires Kafka\n")
    
    # 1. Créer le topic
    print("1. Création du topic...")
    create_kafka_topic()
    
    # 2. Créer un producteur
    print("\n2. Création du producteur...")
    producer = create_kafka_producer()
    
    # 3. Envoyer un message de test
    print("\n3. Envoi d'un message de test...")
    test_message = {
        "transaction_id": "TEST_001",
        "user_id": "USER_TEST",
        "amount": 99.99,
        "timestamp": "2024-01-01T12:00:00"
    }
    
    topic = os.getenv('KAFKA_TOPIC', 'transactions')
    success = send_message(producer, topic, test_message, key="USER_TEST")
    
    if success:
        print("✅ Message envoyé avec succès!")
    else:
        print("❌ Échec de l'envoi")
    
    # 4. Fermer proprement
    print("\n4. Fermeture...")
    flush_and_close_producer(producer)
    
    print(f"\n✅ Test terminé. Vérifiez les logs dans: {os.getenv('LOG_PATH')}")