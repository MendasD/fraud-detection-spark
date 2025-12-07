# Fraud Detection with PySpark & Kafka (Big Data Project)

## Présentation

Ce projet met en place un système de **détection de fraude financière en temps réel** basé sur une architecture Big Data.
Il simule des flux massifs de transactions et utilise **Kafka** pour le streaming et **PySpark** pour le traitement et la détection de fraude.

Le projet reproduit un cas réel de travail d’un **Data Engineer / Data Scientist** en environnement Big Data.

---

## Structure du projet

```markdown
fraud-detection-spark/
├── data/
│   ├── checkpoints/
│   ├── transactions/       
│   ├── historical/         
│   └── models/             
├── logs/
│   └── app.log            
├── notebooks/
│   ├── 01_generate_historical_data.ipynb
│   └── 02_model_training.ipynb
├── src/
│   ├── producers/
│   │   └── transaction_generator.py
│   ├── streaming/
│   │   ├── fraud_detector.py
│   │   └── ml_fraud_detector.py
│   ├── models/
│   │   ├── transactions.py
│   │   ├── train_model.py
│   │   └── feature_engineering.py
│   ├── utils/
│   │   ├── logger.py
│   │   ├── kafka_utils.py
│   │   └── spark_utils.py
│   ├── data_generation/
│   │   └── generate_historical.py  
│   └── dashboard/
│       ├── layouts/
│       └── app.py
├── config/
│   └── config.yaml
├── run_generate_data.py
├── run_generator.py
├── run_train_model.py
├── run_detector_simple.py
├── test_ml_model.py
├── verify_spark_kafka.py
├── launch_system_fixed.bat
├── run_ml_detector.bat
├── start_here.bat
├── sparkEnv/              # Environnement virtuel
├── .env                   # Variables d'environnement
├── docker-compose.yml
├── requirements.txt
├── project-structure.md
├── documentation.html
├── QUICK_START.md
└── README.md
```

---

## Version Python requise

Le projet fonctionne uniquement avec :

**Python 3.10.x**
⚠️ Python 3.11+ peut causer des incompatibilités avec PySpark.

Vérifier votre version :

```bash
python --version
```

---

## Installation

### 1. Créer l’environnement virtuel (à la racine du projet)

Il est recommandé de créer un environnement virtuel nommé **sparkEnv** :

```bash
python -m venv sparkEnv
```

Activer l’environnement :

Sous Windows :

```bash
sparkEnv\Scripts\activate
```

---

### 2. Installer les dépendances Python

```bash
pip install -r requirements.txt
```

---

### 3. Configuration du fichier `.env`

Créer un fichier `.env` à la racine du projet.

Copier la configuration décrite dans **documentation.html**, puis **adapter les chemins selon votre machine**, notamment :

* Chemin vers Java
* Chemin vers Python (environnement virtuel)
* Configuration Kafka

⚠️ Il peut être nécessaire de définir dans le `.env` :

```env
PYTHON_SPARK=C:\chemin\vers\sparkEnv\Scripts\python.exe
PYSPARK_DRIVER_PYTHON=C:\chemin\vers\sparkEnv\Scripts\python.exe
JAVA_HOME=C:\chemin\vers\java
```

---

## Installation de Java

Le projet nécessite **Java 17 ou plus**.

1. Télécharger et installer Java (JDK 17 ou 21 recommandé)
2. Ajouter Java aux variables d’environnement système
3. Mettre à jour la variable suivante dans le fichier `.env` :

```env
JAVA_HOME=C:\Program Files\...
```

---

## Lancer Kafka

### Option 1 — Via Docker (recommandé)

```bash
docker-compose up -d
```

### Option 2 — Installation locale de Kafka (si Docker pose problème)

Si Docker plante ou que le serveur Kafka se coupe :

1. Télécharger Kafka :
   [https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)
   (Version recommandée : **Scala 2.13 – Kafka 3.7.2**)

2. Démarrer le serveur Kafka :

```bash
bin\windows\kafka-server-start.bat config\kraft\server.properties
```

⚠️ Assurez-vous que Kafka et Zookeeper sont bien actifs avant de continuer.

---

## Démarrage du système

### Méthode recommandée ✅

Lancer simplement :

```bash
start_here.bat
```

(Double-cliquer sur le fichier)

Puis suivre les étapes affichées à l’écran.

Avant cela, assurez-vous que :

* L’environnement virtuel est activé
* Kafka est démarré
* Le fichier `.env` est correctement configuré

---

### Lancement manuel (si nécessaire)

Générer les transactions :

```bash
python src/producers/transaction_generator.py
```

Lancer Spark Streaming (règles simples) :

```bash
python src/streaming/fraud_detector.py
```

Entraîner le modèle ML :

```bash
python src/models/train_model.py
```

Lancer la détection ML :

```bash
python src/streaming/ml_fraud_detector.py
```

Lancer le dashboard en temps réel :

```bash
python src/dashboard/app.py
```

---

## Logs

Les journaux sont disponibles ici :

```
logs/app.log
```

---

## Objectif du projet

Ce projet démontre :

* Le traitement de données massives en temps réel
* Une architecture Kafka + Spark
* La détection automatique de fraude financière
* Une architecture proche des standards professionnels

---
