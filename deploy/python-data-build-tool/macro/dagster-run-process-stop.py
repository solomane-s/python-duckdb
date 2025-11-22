#!/usr/bin/env python3
"""Script pour arreter le processus Dagster principal (port 3000)"""

import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PID_FILE = BASE_DIR / "dagster.pid"

print("Arret du processus Dagster principal (port 3000)...")

try:
    # Trouver le PID du processus utilisant le port 3000
    result = subprocess.run(
        ["lsof", "-ti", ":3000"], 
        capture_output=True, 
        text=True
    )
    
    if result.stdout.strip():
        pids = result.stdout.strip().split('\n')
        for pid in pids:
            try:
                subprocess.run(["kill", "-9", pid], check=True)
                print(f"✓ Processus {pid} arrete (port 3000)")
            except subprocess.CalledProcessError:
                print(f"Erreur lors de l arret du processus {pid}")
        
        # Supprimer le fichier PID s il existe
        if PID_FILE.exists():
            PID_FILE.unlink()
        
        print("✓ Processus Dagster principal arrete avec succes.")
    else:
        print("Aucun processus Dagster sur le port 3000.")
        # Nettoyer le fichier PID orphelin
        if PID_FILE.exists():
            PID_FILE.unlink()
            
except FileNotFoundError:
    print("Erreur: commande lsof non trouvee.")
    sys.exit(1)
except Exception as e:
    print(f"Erreur inattendue: {e}")
    sys.exit(1)
