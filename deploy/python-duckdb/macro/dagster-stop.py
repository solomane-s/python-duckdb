#!/usr/bin/env python3
"""Script pour arreter tous les processus Dagster en cours"""

import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PID_FILE = BASE_DIR / "dagster.pid"

print("Arret de tous les processus Dagster...")

try:
    result = subprocess.run(["pkill", "-f", "dagster dev"], capture_output=True, text=True)
    
    # Supprimer le fichier PID s il existe
    if PID_FILE.exists():
        PID_FILE.unlink()
    
    if result.returncode == 0:
        print("✓ Processus Dagster arretes avec succes.")
    elif result.returncode == 1:
        print("Aucun processus Dagster en cours d execution.")
    else:
        print(f"Erreur lors de l arret (code: {result.returncode})")
        sys.exit(result.returncode)
except FileNotFoundError:
    print("Erreur: commande pkill non trouvee.")
    sys.exit(1)
except Exception as e:
    print(f"Erreur inattendue: {e}")
    sys.exit(1)
