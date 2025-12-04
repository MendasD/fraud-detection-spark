@echo off
REM ====================================================================
REM  SYSTEME COMPLET DE DETECTION DE FRAUDES EN TEMPS REEL
REM ====================================================================
REM  Lance le détecteur ML avec Spark + Kafka
REM  Prépare les données pour le dashboard Dash
REM ====================================================================

color 0A
echo.
echo ========================================
echo   FRAUD DETECTION SYSTEM - ML Mode
echo ========================================
echo.

REM Vérifier que l'environnement virtuel existe
if not exist "sparkEnv\Scripts\activate.bat" (
    echo [ERREUR] Environnement virtuel 'sparkEnv' introuvable
    echo Creez-le avec: python -m venv sparkEnv
    pause
    exit /b 1
)

REM Activer l'environnement
call sparkEnv\Scripts\activate

echo [1/5] Verification de l'environnement...
echo.

REM Vérifier que Kafka est accessible
python -c "from kafka import KafkaConsumer; c = KafkaConsumer(bootstrap_servers='localhost:9092', consumer_timeout_ms=3000); print('✅ Kafka OK'); c.close()" 2>nul
if errorlevel 1 (
    echo [ERREUR] Kafka n'est pas accessible
    echo Verifiez que Docker est lance avec: docker-compose up -d
    pause
    exit /b 1
)

echo ✅ Kafka accessible
echo.

REM Vérifier que le modèle existe
if not exist "data\models\random_forest_fraud_detector" (
    echo [ERREUR] Modele ML introuvable
    echo Entrainez d'abord le modele avec: python src\models\train_model.py
    pause
    exit /b 1
)

echo ✅ Modele ML present
echo.

echo [2/5] Configuration Spark...
echo.

REM Packages nécessaires pour Kafka
set PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:3.5.1

echo Packages Spark-Kafka: charges
echo.

echo [3/5] Preparation des dossiers...
echo.

REM Créer les dossiers nécessaires
if not exist "data\transactions" mkdir data\transactions
if not exist "data\checkpoints" mkdir data\checkpoints

echo ✅ Dossiers prets
echo.

echo [4/5] Demarrage du detecteur ML...
echo.
echo Mode: MEMORY (pour dashboard en temps reel)
echo Trigger: 5 secondes
echo Output: Table 'fraud_detection_ml' en memoire
echo.
echo ⚠️  Pour arreter: Ctrl+C
echo.
echo ========================================
echo.

REM Lancer le détecteur avec spark-submit
spark-submit ^
    --packages %PACKAGES% ^
    --master local[*] ^
    --driver-memory 2g ^
    --executor-memory 2g ^
    --conf spark.sql.shuffle.partitions=4 ^
    --conf spark.sql.streaming.checkpointLocation=./data/checkpoints ^
    --conf spark.sql.adaptive.enabled=true ^
    src\streaming\ml_fraud_detector.py ^
    --mode memory ^
    --trigger "5 seconds"

echo.
echo ========================================
echo [5/5] Detecteur arrete
echo ========================================
echo.

REM Si on arrive ici, c'est que le détecteur s'est arrêté
echo Le systeme s'est arrete.
echo.
echo Pour relancer:
echo   1. Verifiez que Kafka tourne: docker-compose ps
echo   2. Relancez ce script: run_fraud_detection_system.bat
echo.

pause