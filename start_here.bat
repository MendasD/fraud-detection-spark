@echo off
REM ====================================================================
REM  GUIDE DE DEMARRAGE PAS A PAS
REM  Plus simple et plus fiable que le lancement automatique
REM ====================================================================

color 0E
cls

echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║                                                            ║
echo ║         FRAUD DETECTION - GUIDE DE DEMARRAGE               ║
echo ║                                                            ║
echo ╚════════════════════════════════════════════════════════════╝
echo.
echo.

echo Ce guide va vous aider a demarrer le systeme etape par etape.
echo.
echo Vous allez ouvrir 3 terminaux :
echo   Terminal 1 : Producteur de transactions
echo   Terminal 2 : Detecteur ML (Spark)
echo   Terminal 3 : Dashboard (Dash)
echo.
echo ═══════════════════════════════════════════════════════════
echo.

pause

:MENU
cls
echo.
echo ╔════════════════════════════════════════════════════════════╗
echo ║                      MENU PRINCIPAL                        ║
echo ╚════════════════════════════════════════════════════════════╝
echo.
echo   [1] Verifier l'environnement
echo   [2] Lancer Terminal 1 - Producteur de transactions
echo   [3] Lancer Terminal 2 - Detecteur ML
echo   [4] Lancer Terminal 3 - Dashboard
echo   [5] Lancer TOUT automatiquement (peut echouer)
echo   [0] Quitter
echo.
echo ═══════════════════════════════════════════════════════════
echo.
set /p choice="Votre choix : "

if "%choice%"=="1" goto VERIFY
if "%choice%"=="2" goto PRODUCER
if "%choice%"=="3" goto DETECTOR
if "%choice%"=="4" goto DASHBOARD
if "%choice%"=="5" goto AUTO
if "%choice%"=="0" goto END
goto MENU

:VERIFY
cls
echo.
echo ═══════════════════════════════════════════════════════════
echo  VERIFICATION DE L'ENVIRONNEMENT
echo ═══════════════════════════════════════════════════════════
echo.

REM Activer l'environnement
call sparkEnv\Scripts\activate

echo [1/5] Environnement virtuel...
if exist "sparkEnv\Scripts\activate.bat" (
    echo ** OK
) else (
    echo * ERREUR - Environnement virtuel introuvable
)
echo.

echo [2/5] Docker...
docker ps >nul 2>&1
if errorlevel 1 (
    echo * ERREUR - Docker n'est pas lance
) else (
    echo ** OK
)
echo.

echo [3/5] Kafka...
docker ps | findstr "kafka" >nul 2>&1
if errorlevel 1 (
    echo * ERREUR - Kafka n'est pas en cours d'execution
    echo    Lancez: docker-compose up -d
) else (
    echo ** OK
)
echo.

echo [4/5] Modele ML...
if exist "data\models\random_forest_fraud_detector" (
    echo ** OK
) else (
    echo * ERREUR - Modele introuvable
)
echo.

echo [5/5] Scripts...
if exist "run_detector_simple.py" (
    echo ** OK
) else (
    echo * ERREUR - run_detector_simple.py manquant
)
echo.

echo ═══════════════════════════════════════════════════════════
echo.
pause
goto MENU

:PRODUCER
echo.
echo ═══════════════════════════════════════════════════════════
echo  LANCEMENT DU PRODUCTEUR (Terminal 1)
echo ═══════════════════════════════════════════════════════════
echo.
echo Un nouveau terminal va s'ouvrir.
echo Il generera des transactions vers Kafka.
echo.
echo NE FERMEZ PAS ce terminal tant que le systeme tourne.
echo.
pause

start "Terminal 1 - Transaction Producer" cmd /k "call sparkEnv\Scripts\activate && python src\producers\transactions_generator.py"

echo.
echo ** Terminal 1 ouvert
echo.
echo Attendez que vous voyiez dans Terminal 1 :
echo   "=== Transactions envoyees vers Kafka... ==="
echo.
pause
goto MENU

:DETECTOR
echo.
echo ═══════════════════════════════════════════════════════════
echo  LANCEMENT DU DETECTEUR ML (Terminal 2)
echo ═══════════════════════════════════════════════════════════
echo.
echo Un nouveau terminal va s'ouvrir.
echo Il analysera les transactions avec le modele ML.
echo.
echo NE FERMEZ PAS ce terminal tant que le systeme tourne.
echo.
echo IMPORTANT : Attendez 15-20 secondes qu'il demarre completement
echo avant de lancer le dashboard (etape suivante).
echo.
pause

start "Terminal 2 - ML Detector" cmd /k "call sparkEnv\Scripts\activate && python run_detector_simple.py --mode memory --trigger "5 seconds""

echo.
echo ** Terminal 2 ouvert
echo.
echo Attendez que vous voyiez dans Terminal 2 :
echo   "** Pipeline ML demarre avec succes"
echo   "==  Appuyez sur Ctrl+C pour arreter =="
echo.
echo Cela peut prendre 15-20 secondes...
echo.
pause
goto MENU

:DASHBOARD
echo.
echo ═══════════════════════════════════════════════════════════
echo  LANCEMENT DU DASHBOARD (Terminal 3)
echo ═══════════════════════════════════════════════════════════
echo.
echo ATTENTION : Assurez-vous que Terminal 2 (Detecteur ML)
echo est completement demarre avant de continuer !
echo.
echo Verifiez dans Terminal 2 que vous voyez :
echo   "** Pipeline ML demarre avec succes"
echo.
pause

echo.
echo Lancement du dashboard...
echo.

call sparkEnv\Scripts\activate
python src\dashboard\app.py

echo.
echo ═══════════════════════════════════════════════════════════
echo  DASHBOARD ARRETE
echo ═══════════════════════════════════════════════════════════
pause
goto MENU

:AUTO
echo.
echo ═══════════════════════════════════════════════════════════
echo  LANCEMENT AUTOMATIQUE
echo ═══════════════════════════════════════════════════════════
echo.
echo Cette methode peut echouer si l'environnement n'est pas
echo correctement configure.
echo.
echo Si cela ne fonctionne pas, utilisez plutot les options
echo 2, 3, et 4 du menu dans l'ordre.
echo.
pause

start launch_system_fixed.bat

echo.
echo Systeme lance. Consultez les fenetres ouvertes.
echo.
pause
goto MENU

:END
echo.
echo ═══════════════════════════════════════════════════════════
echo.
echo Pour arreter le systeme :
echo   1. Fermez Terminal 3 (Dashboard)
echo   2. Fermez Terminal 2 (Detecteur ML)  
echo   3. Fermez Terminal 1 (Producteur)
echo.
echo Puis arretez Docker si necessaire :
echo   docker-compose down
echo.
echo ═══════════════════════════════════════════════════════════
echo.
pause