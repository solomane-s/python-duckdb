#!/usr/bin/env python3
"""
Script pour mettre à jour le dossier archive avec le contenu actuel du projet.
Ce script copie tous les fichiers importants dans le dossier archive.
"""

import os
import shutil
from pathlib import Path
from datetime import datetime


def update_archive():
    """
    Met à jour le dossier archive avec le contenu actuel du projet.
    """
    # Répertoire racine du projet
    project_root = Path(__file__).parent
    archive_dir = project_root / "archive"
    
    # Créer le dossier archive principal s'il n'existe pas
    archive_dir.mkdir(exist_ok=True)
    
    # Créer un sous-dossier avec horodatage
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    timestamped_dir = archive_dir / f"archive - {timestamp}"
    timestamped_dir.mkdir(exist_ok=True)
    
    # Dossiers et fichiers à archiver
    items_to_archive = [
        "deploy",
        "dev",
        "prod",
        "test",
        "python-data-build-tool.py",
        "update-deploy.py",
        "update-dev.py",
        "update-prod.py",
        "update-test.py",
        "update-archive.py",
        "README.md"
    ]
    
    print(f"📦 Mise à jour de l'archive - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    for item in items_to_archive:
        source = project_root / item
        destination = timestamped_dir / item
        
        if not source.exists():
            print(f"⚠️  Ignoré (n'existe pas): {item}")
            continue
        
        try:
            if source.is_dir():
                # Supprimer le dossier de destination s'il existe
                if destination.exists():
                    shutil.rmtree(destination)
                # Copier le dossier
                shutil.copytree(source, destination)
                print(f"✅ Dossier copié: {item}")
            else:
                # Copier le fichier
                shutil.copy2(source, destination)
                print(f"✅ Fichier copié: {item}")
        except Exception as e:
            print(f"❌ Erreur lors de la copie de {item}: {e}")
    
    print("=" * 60)
    print(f"✨ Archive créée dans: {timestamped_dir}")


if __name__ == "__main__":
    update_archive()
