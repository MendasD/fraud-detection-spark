# 🚀 Guide de Démarrage Rapide - Système de Détection de Fraudes ML

## 📋 Prérequis

✅ Vous avez déjà vérifié :
- Spark fonctionne (`verify_spark_kafka.py` OK)
- Kafka est accessible (vérifié)
- Le modèle ML fonctionne (`test_ml_model.py` OK)

## 🎯 Étapes de Lancement

### 1️⃣ Installer les dépendances du dashboard

```bash
pip install -r requirements.txt
```

### 2️⃣ Vérifier que Docker est lancé

```bash
docker-compose up -d
```

Vérifiez que Kafka et Zookeeper tournent :
```bash
docker-compose ps
```

### 3️⃣ Lancer le producteur de transactions

Dans un terminal :
```bash
python src/producers/transactions_generator.py
```

Vous devriez voir : `✅ Transactions envoyées vers Kafka...`

### 4️⃣ Lancer le système complet

**Option A : Script automatique (RECOMMANDÉ)**
```bash
launch_system.bat
```

Ce script va :
- Vérifier que tout est prêt
- Lancer le détecteur ML avec Spark
- Lancer le dashboard Dash
- Ouvrir automatiquement votre navigateur

**Option B : Lancement manuel**

Terminal 1 - Détecteur ML :
```bash
run_fraud_detection_system.bat
```

Terminal 2 - Dashboard :
```bash
python src/dashboard/app.py
```

### 5️⃣ Accéder au dashboard

Ouvrez votre navigateur : **http://localhost:8050**

## 📊 Indicateurs Disponibles

Le dashboard affiche en temps réel :

### KPIs (Indicateurs Clés)
- 📊 **Total Transactions** : Nombre total de transactions traitées
- 🚨 **Fraudes Détectées** : Nombre et % de fraudes identifiées
- 📈 **Score Moyen** : Score de fraude moyen (0-100)
- 💰 **Montant Total** : Somme totale des transactions
- 💸 **Montant Frauduleux** : Montant total des fraudes
- 🎯 **Précision Modèle** : Accuracy du modèle ML
- 🔍 **Précision ML** : Precision (TP / (TP + FP))
- 📡 **Rappel ML** : Recall (TP / (TP + FN))

### Graphiques

1. **📊 Timeline** : Évolution des transactions légitimes vs frauduleuses
2. **📈 Distribution des Scores** : Histogramme des scores de fraude (0-100)
3. **🏪 Fraudes par Catégorie** : Analyse par type de marchand
4. **🗺️ Carte Géographique** : Localisation des fraudes détectées
5. **🎯 Matrice de Confusion** : Performance du modèle ML
6. **🔍 Top 10 Suspects** : Transactions les plus suspectes

## 🔄 Mise à Jour Automatique

Le dashboard se rafraîchit automatiquement **toutes les 5 secondes** pour afficher :
- Les nouvelles transactions
- Les nouvelles détections
- Les métriques à jour

## 🎨 Personnalisation

### Changer l'intervalle de mise à jour

Dans `app.py`, ligne 61 :
```python
dcc.Interval(
    id='interval-component',
    interval=5*1000,  # Modifier ici (en millisecondes)
    n_intervals=0
)
```

### Changer le port du dashboard

```bash
python src/dashboard/app.py --port 8080
```

Ou modifier dans `dashboard_realtime.py`, dernière ligne :
```python
dashboard.run(debug=True, port=8080)
```

## 🐛 Dépannage

### Problème : "Aucune donnée disponible"

**Cause** : Le détecteur ML n'est pas lancé ou pas de transactions

**Solutions** :
1. Vérifiez que le détecteur tourne : cherchez la fenêtre "Fraud Detector ML"
2. Vérifiez que le producteur envoie des transactions
3. Attendez 10-15 secondes après le démarrage

### Problème : Erreur Spark au lancement

**Solution** : Utilisez `spark-submit` au lieu de `python` :
```bash
run_fraud_detection_system.bat
```

### Problème : Dashboard ne se met pas à jour

**Causes possibles** :
1. Le détecteur écrit dans une table différente
2. Spark n'est pas en mode "memory"

**Vérification** :
```bash
# Dans le terminal du détecteur, cherchez :
Mode: memory
Output: Table 'fraud_detection_ml' en mémoire
```

### Problème : Port 8050 déjà utilisé

**Solution** : Changez le port :
```python
dashboard.run(debug=True, port=8051)
```

## 📈 Optimisation des Performances

### Pour plus de transactions par seconde

Dans `ml_fraud_detector.py` :
```python
--trigger "2 seconds"  # Au lieu de 5 secondes
```

### Pour réduire la charge mémoire

Dans `run_fraud_detection_system.bat` :
```batch
--driver-memory 1g  # Au lieu de 2g
--executor-memory 1g
```

## 🛑 Arrêt du Système

1. **Dashboard** : `Ctrl+C` dans le terminal
2. **Détecteur ML** : Fermez la fenêtre "Fraud Detector ML" ou `Ctrl+C`
3. **Producteur** : `Ctrl+C` dans son terminal
4. **Kafka/Zookeeper** : `docker-compose down`

## 📝 Structure des Données

Le détecteur ML écrit dans la table `fraud_detection_ml` avec ces colonnes :

```
- transaction_id      : ID unique
- user_id            : ID utilisateur
- timestamp          : Date/heure
- amount             : Montant
- merchant_id        : ID marchand
- merchant_category  : Catégorie
- location_lat       : Latitude
- location_lon       : Longitude
- is_online          : En ligne ?
- is_fraud           : Label réel (0/1)
- predicted_fraud    : Prédiction ML (0/1)
- fraud_probability  : Probabilité (0-1)
- fraud_score        : Score (0-100)
- risk_level         : SAFE/LOW/MEDIUM/HIGH
- hour_of_day        : Heure (0-23)
- day_of_week        : Jour (1-7)
- true_positive      : Métriques ML
- false_positive
- true_negative
- false_negative
```

## 🎯 Prochaines Étapes

Une fois le système opérationnel, vous pouvez :

1. **Ajuster les seuils** de détection (LOW/MEDIUM/HIGH)
2. **Ajouter des alertes** pour les fraudes HIGH
3. **Exporter les données** vers une base de données
4. **Créer des rapports** automatiques
5. **Améliorer le modèle** avec de nouvelles features

## 💡 Conseils

- Laissez le système tourner 1-2 minutes pour accumuler des données
- Les graphiques sont plus intéressants avec 50+ transactions
- Surveillez la console du détecteur pour voir les logs
- Le dashboard fonctionne mieux avec Chrome/Firefox

## 🆘 Besoin d'Aide ?

Si quelque chose ne fonctionne pas :
1. Vérifiez les logs dans les terminaux
2. Relancez `verify_spark_kafka.py`
3. Vérifiez que tous les services Docker tournent
4. Consultez les messages d'erreur dans la console

Bonne détection de fraudes ! 🛡️