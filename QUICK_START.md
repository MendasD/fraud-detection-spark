# 🚀 Guide de Démarrage Rapide – Système de Détection de Fraudes (ML)

Ce guide vous permet de lancer rapidement le système complet sans entrer dans tous les détails techniques (voir `documentation.html` pour la configuration avancée).

---

## ✅ Pré-requis rapides

Avant de commencer, assurez-vous que :

* Spark fonctionne (`verify_spark_kafka.py`)
* Kafka est accessible
* Le modèle ML est fonctionnel (`test_ml_model.py`)
* L’environnement virtuel est activé

---

## ⚡ Démarrage rapide du système

### 1) Installer les dépendances

```bash
pip install -r requirements.txt
```

### 2) Lancer Kafka

**Option recommandée (Docker)**

```bash
docker-compose up -d
```

Vérification :

```bash
docker-compose ps
```

**Si Docker est instable** (option alternative) :

Télécharger Kafka (Scala 2.13 – version 3.7.2) :
[https://kafka.apache.org/downloads](https://kafka.apache.org/downloads)

Démarrer le serveur :

```bash
bin\windows\kafka-server-start.bat config\kraft\server.properties
```

---

### 3) Lancer le générateur de transactions

Dans un terminal :

```bash
python src/producers/transactions_generator.py
```

Vous devez voir :
`Transactions envoyées vers Kafka...`

---

### 4) Lancer le système complet

**Option recommandée (automatique)**

Double-cliquez sur :

```bash
start_here.bat
```

Ce script vous guide étape par étape et lance :

* le détecteur de fraude (Spark)
* le dashboard
* les vérifications de configuration

**Option manuelle**

Terminal 1 – Détecteur ML :

```bash
run_fraud_detection_system.bat
```

Terminal 2 – Dashboard :

```bash
python src/dashboard/app.py
```

---

### 5) Ouvrir le dashboard

Dans votre navigateur :

```
http://localhost:8050
```

---

## 📊 Indicateurs intégrés

### KPIs affichés en temps réel

* Total des transactions
* Fraudes détectées (nombre + %)
* Score de fraude moyen
* Montant total
* Montant frauduleux
* Accuracy, Precision, Recall du modèle

### Graphiques disponibles

* Timeline des transactions
* Distribution des scores de fraude
* Fraudes par catégorie
* Carte géographique
* Matrice de confusion
* Top 10 transactions suspectes

---

## 🔄 Rafraîchissement automatique

Le dashboard se met à jour automatiquement toutes les **5 secondes**.

---

## 🎨 Personnalisation rapide

### Changer l’intervalle de mise à jour

Dans `dashboard/app.py` :

```python
dcc.Interval(
    interval=5000,  # en millisecondes
)
```

### Changer le port du dashboard

```python
dashboard.run(debug=True, port=8080)
```

Ou en ligne de commande :

```bash
python src/dashboard/app.py --port 8080
```

---

## 📈 Optimisation des performances

### Augmenter la fréquence de traitement

Dans `ml_fraud_detector.py` :

```bash
--trigger "2 seconds"
```

### Réduire la mémoire Spark

Dans `run_ml_detector.bat` (si lancement manuel du detecteur via ce programme) :

```bat
--driver-memory 1g
--executor-memory 1g
```

---

## 🧩 Structure des données en streaming

La table Spark en mémoire s'appelle :

```
fraud_detection_ml
```

Colonnes principales :

```
transaction_id
user_id
timestamp
amount
merchant_id
merchant_category
location_lat
location_lon
is_online
is_fraud
predicted_fraud
fraud_probability
fraud_score
risk_level
hour_of_day
day_of_week
true_positive
false_positive
true_negative
false_negative
```

---

## 🐛 Dépannage rapide

### “Aucune donnée disponible”

Causes probables :

* détecteur ML non lancé
* Kafka inactif
* générateur arrêté

### Erreur Spark

Utiliser le script :

```bash
launch_system_fixed.bat
```

### Le dashboard ne se met pas à jour

Vérifier dans les logs du détecteur :

```
Mode: memory
Table: fraud_detection_ml
```

### Port 8050 déjà utilisé

Changer le port :

```python
dashboard.run(debug=True, port=8051)
```

---

## 🛑 Arrêt du système

Ordre recommandé :

1. Dashboard → `Ctrl + C`
2. Détecteur ML → fermer la fenêtre
3. Générateur de transactions → `Ctrl + C`
4. Kafka :

```bash
docker-compose down
```

---

## 💡 Bonnes pratiques

* Attendre 1 à 2 minutes après le démarrage
* Plus de 50 transactions améliorent la lisibilité
* Surveiller les logs du détecteur

---