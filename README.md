# Fraud Detection with PySpark & Kafka (Big Data Project)

## Présentation

Ce projet met en place un système de **détection de fraude financière en temps réel** basé sur une architecture Big Data.  
Il simule des flux massifs de transactions et utilise **Kafka** pour le streaming et **PySpark** pour le traitement et la détection de fraude.

Ce projet est conçu pour reproduire un cas réel de travail d’un **Data Scientist / Data Engineer** en environnement Big Data.

---

## Structure du projet

```markdown

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
|   |   ├── transactions.py              # Modèle de données, méthodes de sérialisation JSON
│   │   ├── train_model.py              # Entraînement ML
│   │   └── feature_engineering.py      # Features pour détection
│   ├── utils/
|   |   ├── logger.py                  # Configuration du système de logging
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

```

---

## Version Python requise

Ce projet fonctionne uniquement avec :

Python 3.10.x
❌ Python 3.11+ peut causer des incompatibilités avec PySpark et certaines dépendances.

Vérification de la version :

```bash
python --version
```

---

## Installation

Créer un environnement virtuel :

```bash
python -m venv venv
```

Activer l’environnement :

Sous Windows :

```bash
venv\Scripts\activate
```

Installer les dépendances :

```bash
pip install -r requirements.txt
```

---

## Lancer l’infrastructure Kafka

Avec Docker :

```bash
docker-compose up -d
```

---

## Exécution du projet

Générer les transactions (producteur Kafka) :

```bash
python src/producers/transaction_generator.py
```

Lancer le traitement en streaming avec Spark :

```bash
python src/streaming/fraud_detector.py
```

Entraîner le modèle de Machine Learning :

```bash
python src/models/train_model.py
```

Lancer le dashboard temps réel :

```bash
python src/dashboard/app.py
```

---

## Logs

Les journaux de l’application sont stockés ici :

logs/app.log

---

## Objectif du projet

Ce projet montre concrètement :

* Le traitement de données massives en temps réel
* L’architecture Kafka + Spark
* La détection automatique de fraude financière
* Une architecture proche du monde professionnel
