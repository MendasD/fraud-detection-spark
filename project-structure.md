fraud-detection-spark/
├── data/
|   ├── checkpoints/
│   ├── transactions/       # Données générées en temps réel
|   ├── historical/         # Données générées pour l'entraînement du modèle ML
│   └── models/             # Modèles ML sauvegardés
|── logs/
|    ├── app.log            # Logs de l'application
├── notebooks/
│   ├── 01_generate_historical_data.ipynb   # Génération et validation des données historiques
│   └── 02_model_training.ipynb             # Entraînement du modèle ML
├── src/
│   ├── producers/
│   │   └── transaction_generator.py    # Génère et envoie à Kafka
│   ├── streaming/
│   │   ├── fraud_detector.py           # Spark Streaming consumer (détecte sur la base de simples règles)
|   |   └── ml_fraud_detector.py        # Détecte les fraudes en utilisant le modèle ML
│   ├── models/
|   |   ├── transactions.py             # Modèle de données, méthodes de sérialisation JSON
│   │   ├── train_model.py              # Entraînement ML
│   │   └── feature_engineering.py      # Features pour détection
│   ├── utils/
|   |   ├── logger.py                  # Configuration du système de logging
│   │   ├── kafka_utils.py             # Configuration de kafka
│   │   └── spark_utils.py             # Configuration de spark
|   ├── data_generation/
|   |   └── generate_historical.py  
│   └── dashboard/
|       ├── layouts/                    # Différentes pages du dashboard
│       └── app.py                      # partie principale du dashboard
├── config/
│   └── config.yaml
├── run_generate_data.py                # Lancement du générateur de données historiques
├── run_generator.py                    # Lancement du générateur de données en temps réel
├── run_train_model.py                  # Lancement du script pour l'entraînement du modèle ML
├── run_detector_simple.py              # Lancement du detecteur de fraude (ML)
├── test_ml_model.py                    # Test simple du modèle ML (sans Kafka)
├── verify_spark_kafka.py               # Vérification de la configuration de spark et kafka
├── launch_system_fixed.bad             # Lancement complet de tout le système
├── run_ml_detector.bat                 # Lancement du detecteur de fraudes (ML) via spark-submit
├── start_here.bat                      # Guide étape par étape pour le lancement sécurisé du système
|── sparkEnv                            # Environnement virtuel
|── .env                                # Variables d'environnement
├── docker-compose.yml                  # Kafka + Zookeeper
├── requirements.txt
├── project-structure.md                # Structure du projet
├── documentation.html                  # Documentation sur les services utilisés par le projet
├── QUICK_START.md                      # Guide complet pour rapidement démarrer
└── README.md