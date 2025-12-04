@echo off
REM Script pour lancer le détecteur de fraudes ML avec Spark Submit
REM Résout les problèmes de classpath et dépendances

echo ====================================
echo  LANCEMENT DETECTEUR FRAUDES ML
echo ====================================

REM Activer l'environnement virtuel
call sparkEnv\Scripts\activate

echo.
echo [1/3] Verification de Kafka...
python verify_spark_kafka.py
if errorlevel 1 (
    echo ERREUR: Verifiez que Kafka est lance
    pause
    exit /b 1
)

echo.
echo [2/3] Configuration Spark...

REM Définir les packages nécessaires
set PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:3.5.1

echo Packages: %PACKAGES%

echo.
echo [3/3] Lancement du detecteur...
echo Mode: memory (pour dashboard)
echo.

REM Lancer avec spark-submit
spark-submit ^
    --packages %PACKAGES% ^
    --master local[*] ^
    --driver-memory 2g ^
    --executor-memory 2g ^
    --conf spark.sql.shuffle.partitions=4 ^
    src\streaming\ml_fraud_detector.py ^
    --mode memory ^
    --trigger "5 seconds"

echo.
echo ====================================
echo  DETECTEUR ARRETE
echo ====================================
pause