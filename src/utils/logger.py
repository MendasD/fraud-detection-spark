"""
Configuration centralisée du système de logging.
Tous les modules utiliseront ce logger pour avoir des logs cohérents.
"""

import logging
import os
from pathlib import Path
from dotenv import load_dotenv
import sys

# Charger les variables d'environnement
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
    encoding="utf-8"
)

def setup_logger(name: str) -> logging.Logger:
    """
    Configure et retourne un logger avec format standardisé.
    
    Args:
        name: Nom du module qui utilise le logger
        
    Returns:
        Logger configuré
    """
    # Créer le dossier logs s'il n'existe pas
    log_path = Path(os.getenv('LOG_PATH', './logs/app.log'))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Créer le logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Éviter la duplication des handlers si le logger existe déjà
    if logger.handlers:
        return logger
    
    # Format des logs : timestamp - module - niveau - message
    formatter = logging.Formatter(
        '[%(asctime)s - %(name)s] - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler pour fichier (tous les logs)
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Handler pour console (INFO et plus)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Ajouter les handlers au logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


# Logger global pour les utilitaires
logger = setup_logger('fraud_detection')


if __name__ == "__main__":
    # Test du logger
    test_logger = setup_logger('test')
    test_logger.debug("Message de debug")
    test_logger.info("Message d'information")
    test_logger.warning("Message d'avertissement")
    test_logger.error("Message d'erreur")

    # logging path
    log_path = Path(os.getenv('LOG_PATH', './logs/app.log')).resolve()
    print(f"\nLogs sauvegardés dans: {log_path})")