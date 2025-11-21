#!/usr/bin/env python3
"""Script pour lancer Dagster avec le fichier d orchestration du sample"""

import subprocess
import sys
import os
from pathlib import Path

# Chemin automatique vers le fichier dagster-run-process-sample.py
BASE_DIR = Path(__file__).resolve().parent
DAGSTER_FILE = BASE_DIR / "dagster-run-process-sample.py"
LOG_FILE = BASE_DIR / "dagster.log"
PID_FILE = BASE_DIR / "dagster.pid"

if not DAGSTER_FILE.exists():
    print(f"Erreur: Le fichier {DAGSTER_FILE} n existe pas")
    sys.exit(1)

# Verifier si Dagster est deja en cours
if PID_FILE.exists():
    try:
        with open(PID_FILE, "r") as f:
            old_pid = int(f.read().strip())
        # Verifier si le processus existe
        os.kill(old_pid, 0)
        print(f"Dagster est deja en cours d execution (PID: {old_pid})")
        print(f"Utilisez dagster-stop.py pour l arreter d abord.")
        sys.exit(1)
    except (OSError, ValueError):
        # Le processus n existe plus, supprimer le fichier PID
        PID_FILE.unlink()

print(f"Lancement de Dagster avec: {DAGSTER_FILE}")
print(f"Interface UI disponible sur: http://localhost:3001")
print(f"Logs: {LOG_FILE}")
print()
print("Dagster sera lance en arriere-plan (port 3001).")
print("Utilisez dagster-stop.py pour l arreter.")
print()

try:
    # Lancer en arriere-plan avec nohup sur le port 3001
    with open(LOG_FILE, "w") as log:
        process = subprocess.Popen(
            ["nohup", "dagster", "dev", "-f", str(DAGSTER_FILE), "--port", "3001"],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True
        )
    
    # Sauvegarder le PID
    with open(PID_FILE, "w") as f:
        f.write(str(process.pid))
    
    print(f"✓ Dagster lance avec succes (PID: {process.pid})")
    print(f"✓ Vous pouvez fermer ce terminal en toute securite.")
    
except FileNotFoundError:
    print("Erreur: dagster n est pas installe.")
    print("Installez-le avec: pip install dagster dagster-webserver")
    sys.exit(1)
except Exception as e:
    print(f"Erreur lors du lancement: {e}")
    sys.exit(1)
