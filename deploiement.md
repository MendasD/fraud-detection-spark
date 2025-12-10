# Guide de Déploiement Docker Complet

## Vue d'ensemble

Ce guide explique comment déployer l'ensemble du système de détection de fraudes en utilisant Docker et Docker Compose. Cette approche permet de déployer le système complet (Kafka, Spark, ML, Dashboard) sur n'importe quelle plateforme supportant Docker.

## Architecture du déploiement

```
┌─────────────────────────────────────────────────────────────┐
│                    DOCKER COMPOSE                            │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  Zookeeper   │───▶│    Kafka     │◀───│  Producer    │  │
│  │  Port: 2181  │    │  Port: 9092  │    │ (Container)  │  │
│  └──────────────┘    └───────┬──────┘    └──────────────┘  │
│                              │                               │
│  ┌──────────────┐    ┌───────▼──────┐    ┌──────────────┐  │
│  │ Spark Master │◀───│  Detector    │    │  Dashboard   │  │
│  │  Port: 8080  │    │ (Spark Job)  │───▶│ Port: 8050   │  │
│  └──────┬───────┘    └──────────────┘    └──────────────┘  │
│         │                                                    │
│  ┌──────▼───────┐                                           │
│  │ Spark Worker │                                           │
│  │              │                                           │
│  └──────────────┘                                           │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Prérequis

### Logiciels nécessaires

- **Docker Desktop** 24.0+
  - Windows : [https://docs.docker.com/desktop/install/windows-install/](https://docs.docker.com/desktop/install/windows-install/)
  - Mac : [https://docs.docker.com/desktop/install/mac-install/](https://docs.docker.com/desktop/install/mac-install/)
  - Linux : [https://docs.docker.com/desktop/install/linux-install/](https://docs.docker.com/desktop/install/linux-install/)

- **Docker Compose** 2.20+ (inclus dans Docker Desktop)

### Configuration système minimale

```
CPU : 4 cœurs recommandés
RAM : 8 GB minimum (12 GB recommandé)
Disque : 10 GB disponible
```

### Ports nécessaires (doivent être libres)

```
2181  : Zookeeper
9092  : Kafka (externe)
29092 : Kafka (interne)
8080  : Spark Master UI
7077  : Spark Master
4040  : Spark Application UI
8050  : Dashboard
```

## Structure des fichiers

Créez ces fichiers à la racine de votre projet :

```
fraud-detection-spark/
├── docker-compose.yml          # Orchestration des services
├── Dockerfile.producer         # Image du producteur
├── Dockerfile.detector         # Image du détecteur Spark
├── Dockerfile.dashboard        # Image du dashboard
├── .dockerignore              # Fichiers à ignorer
├── src/                       # Code source
├── data/                      # Données et modèles
└── requirements.txt           # Dépendances Python
```

## Fichier .dockerignore

Créez un fichier `.dockerignore` pour optimiser les builds :

```
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
sparkEnv/
venv/
env/

# Data
data/transactions/
data/checkpoints/
*.parquet

# Logs
logs/
*.log

# IDE
.vscode/
.idea/
*.swp

# OS
.DS_Store
Thumbs.db

# Git
.git/
.gitignore

# Documentation
*.md
documentation.pdf
```

## Étapes de déploiement

### 1. Préparation

#### A. Vérifier Docker

```bash
# Vérifier Docker
docker --version
# Sortie attendue : Docker version 24.0.x

# Vérifier Docker Compose
docker-compose --version
# Sortie attendue : Docker Compose version 2.20.x

# Tester Docker
docker run hello-world
```

#### B. Préparer le modèle ML

Le modèle ML doit être entraîné avant le déploiement :

```bash
# Activer l'environnement virtuel
.\sparkEnv\Scripts\Activate.ps1  # Windows
source sparkEnv/bin/activate      # Linux/Mac

# Générer les données historiques
python run_generate_data.py

# Entraîner le modèle
python run_train_model.py

# Vérifier que le modèle existe
ls data/models/random_forest_fraud_detector/
```

### 2. Build des images Docker

```bash
# Builder toutes les images
docker-compose build

# Ou builder image par image
docker-compose build producer
docker-compose build detector
docker-compose build dashboard
```

**Temps estimé** : 5-10 minutes selon votre connexion internet

### 3. Lancement du système

#### Démarrage complet

```bash
# Démarrer tous les services
docker-compose up -d

# Vérifier que tous les conteneurs sont lancés
docker-compose ps
```

**Sortie attendue** :

```
NAME                IMAGE                              STATUS
zookeeper           confluentinc/cp-zookeeper:7.5.0   Up (healthy)
kafka               confluentinc/cp-kafka:7.5.0       Up (healthy)
spark-master        bitnami/spark:3.5.0               Up
spark-worker        bitnami/spark:3.5.0               Up
fraud-producer      fraud-detection-producer          Up
fraud-detector      fraud-detection-detector          Up
fraud-dashboard     fraud-detection-dashboard         Up
```

#### Démarrage séquentiel (recommandé pour la première fois)

```bash
# 1. Infrastructure (Zookeeper + Kafka)
docker-compose up -d zookeeper kafka

# Attendre 30 secondes
timeout /t 30  # Windows
sleep 30       # Linux/Mac

# 2. Spark
docker-compose up -d spark-master spark-worker

# Attendre 20 secondes
timeout /t 20  # Windows
sleep 20       # Linux/Mac

# 3. Producteur
docker-compose up -d producer

# Attendre 10 secondes
timeout /t 10  # Windows
sleep 10       # Linux/Mac

# 4. Détecteur
docker-compose up -d detector

# Attendre 30 secondes
timeout /t 30  # Windows
sleep 30       # Linux/Mac

# 5. Dashboard
docker-compose up -d dashboard
```

### 4. Vérification du déploiement

#### Vérifier les logs

```bash
# Logs de tous les services
docker-compose logs -f

# Logs d'un service spécifique
docker-compose logs -f producer
docker-compose logs -f detector
docker-compose logs -f dashboard
docker-compose logs -f kafka
```

#### Vérifier Kafka

```bash
# Lister les topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

# Devrait afficher :
# transactions

# Voir les messages
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --from-beginning \
  --max-messages 5
```

#### Vérifier Spark

Ouvrez dans votre navigateur :
- **Spark Master UI** : [http://localhost:8080](http://localhost:8080)
- **Spark Application UI** : [http://localhost:4040](http://localhost:4040)

#### Vérifier le Dashboard

Ouvrez dans votre navigateur :
- **Dashboard** : [http://localhost:8050](http://localhost:8050)

**Note** : Le dashboard peut prendre 1-2 minutes pour afficher des données.

### 5. Surveillance

#### État des conteneurs

```bash
# Voir l'état
docker-compose ps

# Voir l'utilisation des ressources
docker stats
```

#### Logs en temps réel

```bash
# Tous les logs
docker-compose logs -f

# Filtrer par service
docker-compose logs -f producer detector dashboard
```

## Commandes utiles

### Gestion des services

```bash
# Arrêter tous les services
docker-compose down

# Arrêter et supprimer les volumes
docker-compose down -v

# Redémarrer un service
docker-compose restart producer

# Arrêter un service
docker-compose stop detector

# Démarrer un service arrêté
docker-compose start detector

# Reconstruire et redémarrer
docker-compose up -d --build
```

### Debugging

```bash
# Entrer dans un conteneur
docker exec -it fraud-detector bash
docker exec -it fraud-dashboard bash

# Voir les logs détaillés
docker-compose logs --tail=100 detector

# Inspecter un conteneur
docker inspect fraud-detector

# Voir les processus dans un conteneur
docker top fraud-detector
```

### Nettoyage

```bash
# Supprimer tous les conteneurs arrêtés
docker container prune

# Supprimer toutes les images non utilisées
docker image prune -a

# Nettoyage complet (ATTENTION : supprime tout)
docker system prune -a --volumes
```

## Options de déploiement cloud

### Option 1 : Railway.app (Gratuit)

**Avantages** :
- 500h gratuites par mois
- Déploiement automatique depuis GitHub
- Support Docker natif

**Étapes** :
1. Créer un compte sur [railway.app](https://railway.app)
2. Connecter votre dépôt GitHub
3. Railway détecte automatiquement `docker-compose.yml`
4. Déployer

**Limitations** :
- 512 MB RAM par service (suffisant pour notre cas)
- Nécessite optimisation des ressources

### Option 2 : Render.com (Gratuit)

**Avantages** :
- Services gratuits persistants
- Support Docker
- SSL automatique

**Étapes** :
1. Créer un compte sur [render.com](https://render.com)
2. Créer un "Web Service" par composant
3. Configurer depuis Docker

**Limitations** :
- Services gratuits dorment après inactivité
- Démarrage lent (cold start)

### Option 3 : Google Cloud Run (Crédit gratuit)

**Avantages** :
- $300 de crédit gratuit
- Scalabilité automatique
- Pay-per-use

**Étapes** :
```bash
# Installer gcloud CLI
# https://cloud.google.com/sdk/docs/install

# Authentification
gcloud auth login

# Configurer le projet
gcloud config set project YOUR_PROJECT_ID

# Build et push des images
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/fraud-dashboard

# Déployer
gcloud run deploy fraud-dashboard \
  --image gcr.io/YOUR_PROJECT_ID/fraud-dashboard \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```

### Option 4 : Azure Container Instances (Crédit gratuit)

**Avantages** :
- $200 de crédit gratuit
- Simple à utiliser
- Support Docker Compose

**Étapes** :
```bash
# Installer Azure CLI
# https://docs.microsoft.com/cli/azure/install-azure-cli

# Login
az login

# Créer un groupe de ressources
az group create --name fraud-detection --location eastus

# Déployer depuis docker-compose
az container create --resource-group fraud-detection \
  --file docker-compose.yml
```

## Optimisation pour le déploiement

### Réduire la consommation mémoire

Modifiez `docker-compose.yml` :

```yaml
services:
  spark-worker:
    environment:
      SPARK_WORKER_MEMORY: 512m  # Au lieu de 2G
      SPARK_WORKER_CORES: 1      # Au lieu de 2
  
  detector:
    environment:
      SPARK_DRIVER_MEMORY: 512m
      SPARK_EXECUTOR_MEMORY: 512m
```

### Optimiser les images Docker

```bash
# Utiliser des images slim
FROM python:3.10-slim  # Au lieu de python:3.10

# Multi-stage builds
FROM python:3.10 AS builder
# ... install dependencies
FROM python:3.10-slim
COPY --from=builder ...
```

## Troubleshooting

### Problème : Conteneur s'arrête immédiatement

```bash
# Voir les logs
docker-compose logs container_name

# Vérifier les healthchecks
docker inspect container_name | grep -A 10 Health
```

### Problème : Kafka non accessible

```bash
# Vérifier que Kafka est healthy
docker-compose ps kafka

# Tester la connexion
docker exec -it kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9092
```

### Problème : Dashboard affiche "En attente de données"

```bash
# Vérifier que le détecteur tourne
docker-compose logs detector

# Vérifier la table Spark en mémoire
docker exec -it fraud-detector python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql('SHOW TABLES').show()
"
```

### Problème : Mémoire insuffisante

```bash
# Augmenter la mémoire Docker Desktop
# Settings → Resources → Memory → 8 GB minimum

# Ou réduire les services
docker-compose up -d zookeeper kafka producer detector dashboard
# (sans spark-master et spark-worker, utiliser mode local)
```

## Monitoring et métriques

### Prometheus + Grafana (Optionnel)

Ajoutez à `docker-compose.yml` :

```yaml
  prometheus:
    image: prom/prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml

  grafana:
    image: grafana/grafana
    ports:
      - "3000:3000"
    depends_on:
      - prometheus
```

## Sécurité

### En production

1. **Variables d'environnement**
   ```bash
   # Utiliser des secrets
   docker-compose --env-file .env.production up -d
   ```

2. **Network isolation**
   ```yaml
   networks:
     frontend:
     backend:
       internal: true
   ```

3. **Limites de ressources**
   ```yaml
   deploy:
     resources:
       limits:
         cpus: '0.5'
         memory: 512M
   ```
