@echo off
REM ====================================================================
REM  LANCEMENT COMPLET DU SYSTEME DE DETECTION DE FRAUDES
REM ====================================================================
REM  1. Vérifie que tout est prêt (Kafka, modèle, etc.)
REM  2. Lance le détecteur ML (Spark + Kafka)
REM  3. Lance le dashboard Dash en temps réel
REM ====================================================================

color 0B
cls

echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║                                                            ║
echo ║      FRAUD DETECTION SYSTEM - Real-Time ML Edition        ║
echo ║                                                            ║
echo ╚════════════════════════════════════════════════════════════╝
echo.

REM Activer l'environnement virtuel
if not exist "sparkEnv\Scripts\activate.bat" (
    echo [ERREUR] Environnement virtuel introuvable
    pause
    exit /b 1
)

call sparkEnv\Scripts\activate

echo.
echo ═══════════════════════════════════════════════════════════
echo  ETAPE 1/4 : Verification de l'environnement
echo ═══════════════════════════════════════════════════════════
echo.

REM Vérifier Docker (Kafka + Zookeeper)
docker ps >nul 2>&1
if errorlevel 1 (
    echo [ERREUR] Docker n'est pas lance ou accessible
    echo.
    echo Lancez Docker Desktop puis executez:
    echo   docker-compose up -d
    echo.
    pause
    exit /b 1
)

echo ✅ Docker est actif
echo.

REM Vérifier que Kafka tourne
docker ps | findstr "kafka" >nul 2>&1
if errorlevel 1 (
    echo [ERREUR] Kafka n'est pas en cours d'execution
    echo.
    echo Lancez les services avec:
    echo   docker-compose up -d
    echo.
    pause
    exit /b 1
)

echo ✅ Kafka est en cours d'execution
echo.

REM Vérifier le modèle ML
if not exist "data\models\random_forest_fraud_detector" (
    echo [ERREUR] Modele ML introuvable
    echo.
    echo Entrainez le modele avec:
    echo   python src\models\train_model.py
    echo.
    pause
    exit /b 1
)

echo ✅ Modele ML present
echo.

REM Vérifier les dépendances Python
python -c "import dash; import plotly" >nul 2>&1
if errorlevel 1 (
    echo [ERREUR] Dependances manquantes pour le dashboard
    echo.
    echo Installez avec:
    echo   pip install dash plotly
    echo.
    pause
    exit /b 1
)

echo ✅ Dependances Python OK
echo.

echo.
echo ═══════════════════════════════════════════════════════════
echo  ETAPE 2/4 : Preparation des dossiers
echo ═══════════════════════════════════════════════════════════
echo.

if not exist "data\transactions" mkdir data\transactions
if not exist "data\checkpoints" mkdir data\checkpoints

echo ✅ Dossiers prepares
echo.

echo.
echo ═══════════════════════════════════════════════════════════
echo  ETAPE 3/4 : Demarrage du detecteur ML (Spark Streaming)
echo ═══════════════════════════════════════════════════════════
echo.
echo Mode: MEMORY (pour le dashboard)
echo Trigger: 5 secondes
echo.
echo Ce processus va tourner en arriere-plan...
echo.

REM Packages Spark-Kafka
set PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:3.5.1

REM Lancer le détecteur en arrière-plan
start "Fraud Detector ML" cmd /k "call sparkEnv\Scripts\activate && spark-submit --packages %PACKAGES% --master local[*] --driver-memory 2g --executor-memory 2g --conf spark.sql.shuffle.partitions=4 src\streaming\ml_fraud_detector.py --mode memory --trigger "5 seconds""

echo ✅ Detecteur ML demarre dans une nouvelle fenetre
echo.
echo ⏳ Attente de 15 secondes pour initialisation Spark...
timeout /t 15 /nobreak >nul
echo.

echo.
echo ═══════════════════════════════════════════════════════════
echo  ETAPE 4/4 : Demarrage du dashboard temps reel
echo ═══════════════════════════════════════════════════════════
echo.
echo Le dashboard sera accessible sur:
echo   👉 http://localhost:8050
echo.
echo ⚠️  Pour arreter le systeme complet:
echo   1. Fermez cette fenetre
echo   2. Fermez la fenetre "Fraud Detector ML"
echo.
echo ═══════════════════════════════════════════════════════════
echo.

REM Lancer le dashboard (bloquant)
python src\dashboard\app.py

echo.
echo ═══════════════════════════════════════════════════════════
echo  SYSTEME ARRETE
echo ═══════════════════════════════════════════════════════════
echo.
pause