"""
Script pour corriger le problème de lecture Parquet sur Windows.
Télécharge les bonnes versions de winutils.exe et hadoop.dll.
"""

import urllib.request
import os
from pathlib import Path
import sys

def download_hadoop_files():
    """Télécharge winutils.exe et hadoop.dll compatibles."""
    
    print("="*60)
    print(" CORRECTION HADOOP POUR PARQUET SUR WINDOWS")
    print("="*60)
    
    # Créer le dossier
    hadoop_bin = Path("C:/hadoop/bin")
    hadoop_bin.mkdir(parents=True, exist_ok=True)
    print(f"\n* Dossier: {hadoop_bin}")
    
    # URLs pour Hadoop 3.3.6 (compatible Spark 4.x)
    files_to_download = {
        'winutils.exe': 'https://github.com/steveloughran/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe',
        'hadoop.dll': 'https://github.com/steveloughran/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll'
    }
    
    print("\n** Téléchargement des fichiers...")
    
    for filename, url in files_to_download.items():
        file_path = hadoop_bin / filename
        
        if file_path.exists():
            print(f"   {filename} existe déjà")
            continue
        
        try:
            print(f"  **** Téléchargement de {filename}...")
            urllib.request.urlretrieve(url, file_path)
            print(f"  ** {filename} téléchargé avec succès")
        except Exception as e:
            print(f"  * Erreur pour {filename}: {e}")
            print(f"\n  * Téléchargez manuellement depuis:")
            print(f"     {url}")
            print(f"     Placez-le dans: {file_path}")
            return False
    
    # Vérifier les fichiers
    print("\n Vérification...")
    all_good = True
    for filename in files_to_download.keys():
        file_path = hadoop_bin / filename
        if file_path.exists():
            size = file_path.stat().st_size / 1024  # Ko
            print(f"   {filename} ({size:.1f} Ko)")
        else:
            print(f"   {filename} manquant!")
            all_good = False
    
    if not all_good:
        return False
    
    # Configurer les variables d'environnement
    print("\n Configuration des variables d'environnement...")
    
    os.environ['HADOOP_HOME'] = 'C:/hadoop'
    os.environ['PATH'] = f"C:/hadoop/bin;{os.environ.get('PATH', '')}"
    
    print(f"   HADOOP_HOME = C:/hadoop")
    print(f"   PATH mis à jour")
    
    # Instructions
    print("\n" + "="*60)
    print(" INSTALLATION TERMINÉE !")
    print("\n" + "="*60)

    return True

if __name__ == "__main__":
    success = download_hadoop_files()
    sys.exit(0 if success else 1)