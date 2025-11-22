#!/usr/bin/env python3
"""Script pour regenerer le projet python-duckdb dans le dossier deploy"""

import subprocess
import sys
from pathlib import Path

# Chemins
BASE_DIR = Path(__file__).resolve().parent
DEPLOY_DIR = BASE_DIR / "deploy"
GENERATOR_SCRIPT = BASE_DIR / "python-data-build-tool.py"

# Vérifier que le script générateur existe
if not GENERATOR_SCRIPT.exists():
    print(f"Erreur: Le fichier {GENERATOR_SCRIPT} n'existe pas")
    sys.exit(1)

print("=" * 60)
print("RESET DEPLOY - Regeneration du projet")
print("=" * 60)
print()
print(f"Dossier cible: {DEPLOY_DIR}")
print(f"Suppression de l'ancien projet...")

# Supprimer l'ancien dossier python-duckdb s'il existe
old_project = DEPLOY_DIR / "python-duckdb"
if old_project.exists():
    import shutil
    shutil.rmtree(old_project)
    print(f"✓ Ancien projet supprime")

# Créer le dossier deploy s'il n'existe pas
DEPLOY_DIR.mkdir(exist_ok=True)

# Lancer la génération
print(f"\nGeneration du nouveau projet...")
print()

try:
    result = subprocess.run(
        [sys.executable, str(GENERATOR_SCRIPT)],
        cwd=str(DEPLOY_DIR),
        check=True
    )
    print()
    print("=" * 60)
    print("✓ Projet regenere avec succes dans deploy/")
    print("=" * 60)
except subprocess.CalledProcessError as e:
    print(f"\nErreur lors de la generation: {e}")
    sys.exit(1)
except Exception as e:
    print(f"\nErreur inattendue: {e}")
    sys.exit(1)
