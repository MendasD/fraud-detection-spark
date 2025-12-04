@echo off
REM ====================================================================
REM  LANCEMENT COMPLET - VERSION CORRIGEE
REM  Utilise Python directement au lieu de spark-submit
REM ====================================================================

color 0B
cls

echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║                                                            ║
echo ║      FRAUD DETECTION SYSTEM - Real-Time ML Edition        ║
echo ║                    VERSION CORRIGEE                        ║
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

REM Vérifier Docker
docker ps >nul 2>&1
if errorlevel 1 (
    echo [ERREUR] Docker n'est pas lance
    pause
    exit /b 1
)

echo ** Docker actif
echo.

REM Vérifier Kafka
docker ps | findstr "kafka" >nul 2>&1
if errorlevel 1 (
    echo [ERREUR] Kafka n'est pas en cours d'execution
    echo.
    echo Lancez: docker-compose up -d
    pause
    exit /b 1
)

echo ** Kafka en cours d'execution
echo.

REM Vérifier le modèle
if not exist "data\models\random_forest_fraud_detector" (
    echo [ERREUR] Modele ML introuvable
    pause
    exit /b 1
)

echo ** Modele ML present
echo.

echo.
echo ═══════════════════════════════════════════════════════════
echo  ETAPE 2/4 : Preparation
echo ═══════════════════════════════════════════════════════════
echo.

if not exist "data\transactions" mkdir data\transactions
if not exist "data\checkpoints" mkdir data\checkpoints

echo ** Dossiers prepares
echo.

echo.
echo ═══════════════════════════════════════════════════════════
echo  ETAPE 3/4 : Demarrage du detecteur ML
echo ═══════════════════════════════════════════════════════════
echo.
echo Mode: PARQUET
echo Trigger: 5 secondes
echo.

REM Lancer le détecteur en arrière-plan avec Python directement (en mode parquet pour stocker les données dans le dossier prevu)
start "Fraud Detector ML" cmd /k "call sparkEnv\Scripts\activate && python run_detector_simple.py --mode parquet --trigger "5 seconds""

echo ** Detecteur ML demarre (nouvelle fenetre)
echo.
echo == Attente de 20 secondes pour initialisation... ==
timeout /t 20 /nobreak >nul
echo.

echo.
echo ═══════════════════════════════════════════════════════════
echo  ETAPE 4/4 : Demarrage du dashboard
echo ═══════════════════════════════════════════════════════════
echo.
echo Dashboard accessible sur: http://localhost:8050
echo.
echo *  Pour arreter:
echo   - Fermez cette fenetre
echo   - Fermez la fenetre "Fraud Detector ML"
echo.
echo ═══════════════════════════════════════════════════════════
echo.

REM Lancer le dashboard
python src\dashboard\app.py

echo.
echo ═══════════════════════════════════════════════════════════
echo  SYSTEME ARRETE
echo ═══════════════════════════════════════════════════════════
pause