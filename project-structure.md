fraud-detection-spark/
├── data/
│   ├── transactions/       # Données générées
│   └── models/             # Modèles ML sauvegardés
|── logs/
|    ├── app.log            # Logs de l'application
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_model_training.ipynb
│   └── 03_performance_analysis.ipynb
├── src/
│   ├── producers/
│   │   └── transaction_generator.py    # Génère et envoie à Kafka
│   ├── streaming/
│   │   └── fraud_detector.py           # Spark Streaming consumer
│   ├── models/
│   │   ├── train_model.py              # Entraînement ML
│   │   └── feature_engineering.py      # Features pour détection
│   ├── utils/
|    |   ├── logger.py                  # Configuration du système de logging
│   │   ├── kafka_utils.py
│   │   └── spark_utils.py
│   └── dashboard/
│       └── app.py                      # Dashboard temps réel
├── config/
│   └── config.yaml
|── .env                                # Variables d'environnement
├── docker-compose.yml                  # Kafka + Zookeeper
├── requirements.txt
└── README.md