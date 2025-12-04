"""
Script pour configurer Hadoop/Spark sur Windows.
Télécharge winutils.exe et configure les variables d'environnement.
"""

import os
import sys
import urllib.request
from pathlib import Path

def setup_hadoop_windows():
    """Configure Hadoop pour Windows en téléchargeant winutils.exe"""
    
    print("=" * 60)
    print("Configuration de Hadoop pour Windows")
    print("=" * 60)
    
    # 1. Créer le dossier hadoop/bin
    hadoop_home = Path("C:/hadoop")
    bin_dir = hadoop_home / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n1. Dossier créé: {bin_dir}")
    
    # 2. Télécharger winutils.exe
    winutils_path = bin_dir / "winutils.exe"
    
    if winutils_path.exists():
        print(f"\n2. winutils.exe existe déjà: {winutils_path}")
    else:
        print(f"\n2. Téléchargement de winutils.exe...")
        
        # URL de winutils pour Hadoop 3.x (compatible avec Spark 3.x/4.x)
        url = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe"
        
        try:
            urllib.request.urlretrieve(url, winutils_path)
            print(f"   Téléchargé: {winutils_path}")
        except Exception as e:
            print(f"   ERREUR: {e}")
            print("\n   Solution alternative:")
            print(f"   1. Téléchargez manuellement depuis: {url}")
            print(f"   2. Placez-le dans: {winutils_path}")
            return False
    
    # 3. Télécharger hadoop.dll (optionnel mais recommandé)
    hadoop_dll_path = bin_dir / "hadoop.dll"
    
    if not hadoop_dll_path.exists():
        print(f"\n3. Téléchargement de hadoop.dll...")
        url_dll = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll"
        
        try:
            urllib.request.urlretrieve(url_dll, hadoop_dll_path)
            print(f"   Téléchargé: {hadoop_dll_path}")
        except Exception as e:
            print(f"   ERREUR (non critique): {e}")
    
    # 4. Configurer les variables d'environnement
    print("\n4. Configuration des variables d'environnement...")
    
    hadoop_home_str = str(hadoop_home)
    
    # Définir HADOOP_HOME
    os.environ['HADOOP_HOME'] = hadoop_home_str
    print(f"   HADOOP_HOME = {hadoop_home_str}")
    
    # Ajouter au PATH
    current_path = os.environ.get('PATH', '')
    bin_path = str(bin_dir)
    
    if bin_path not in current_path:
        os.environ['PATH'] = f"{bin_path};{current_path}"
        print(f"   PATH mis à jour avec: {bin_path}")
    
    # 5. Créer un dossier tmp pour Spark
    tmp_dir = hadoop_home / "tmp"
    tmp_dir.mkdir(exist_ok=True)
    print(f"\n5. Dossier tmp créé: {tmp_dir}")
    
    # 6. Instructions pour rendre permanent
    print("\n" + "=" * 60)
    print("CONFIGURATION TERMINÉE !")
    print("=" * 60)
    print("\nPour rendre cette configuration PERMANENTE,")
    print("ajoutez ces lignes dans votre fichier .env :\n")
    print(f"HADOOP_HOME={hadoop_home_str}")
    print(f"PATH=%PATH%;{bin_path}")
    
    print("\nOu exécutez ces commandes PowerShell (en admin) :\n")
    print(f'[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "{hadoop_home_str}", "Machine")')
    print(f'$path = [System.Environment]::GetEnvironmentVariable("PATH", "Machine")')
    print(f'[System.Environment]::SetEnvironmentVariable("PATH", "$path;{bin_path}", "Machine")')
    
    print("\n" + "=" * 60)
    print("Vous pouvez maintenant lancer le détecteur !")
    print("=" * 60)
    
    return True


if __name__ == "__main__":
    success = setup_hadoop_windows()
    
    if success:
        sys.exit(0)
    else:
        print("\nÉchec de la configuration. Veuillez suivre les instructions.")
        sys.exit(1)