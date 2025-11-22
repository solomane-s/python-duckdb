import os
from pathlib import Path
import shutil
import textwrap
import subprocess
import sys


def check_and_install_dependencies(db_info):
    """Vérifie et propose d'installer les dépendances manquantes"""
    required_packages = {
        'pandas': 'pandas',
        'dagster': 'dagster',
        'dagster-webserver': 'dagster-webserver'
    }
    
    # Ajouter duckdb seulement si choix 1
    if db_info['module'] == 'duckdb':
        required_packages['duckdb'] = 'duckdb'
    
    missing_packages = []
    
    # Vérifier chaque package
    for package_name, pip_name in required_packages.items():
        try:
            __import__(package_name.replace('-', '_'))
        except ImportError:
            missing_packages.append(pip_name)
    
    if missing_packages:
        print("=" * 60)
        print("⚠️  DÉPENDANCES MANQUANTES")
        print("=" * 60)
        print()
        print("Les packages suivants sont nécessaires mais non installés:")
        for pkg in missing_packages:
            print(f"  - {pkg}")
        print()
        
        response = input("Voulez-vous les installer maintenant ? (o/n) : ").lower().strip()
        
        if response in ['o', 'oui', 'y', 'yes']:
            print()
            print("Installation en cours...")
            print()
            try:
                subprocess.check_call([
                    sys.executable, "-m", "pip", "install"
                ] + missing_packages)
                print()
                print("✅ Installation terminée avec succès!")
                print()
            except subprocess.CalledProcessError:
                print()
                print("❌ Erreur lors de l'installation.")
                print("Veuillez installer manuellement avec:")
                print(f"  pip install {' '.join(missing_packages)}")
                print()
                sys.exit(1)
        else:
            print()
            print("Installation annulée.")
            print("Pour installer manuellement:")
            print(f"  pip install {' '.join(missing_packages)}")
            print()
            print("Ou installez depuis requirements.txt:")
            print("  pip install -r requirements.txt")
            print()
            sys.exit(1)
        
        print("=" * 60)
        print()


# Choix du type de base de données
def choose_database_type():
    """Demande à l'utilisateur de choisir le type de base de données"""
    print("="*60)
    print("CHOIX DU TYPE DE BASE DE DONNÉES")
    print("="*60)
    print()
    print("Veuillez choisir le type de base de données à utiliser:")
    print()
    print("  [1] DuckDB    - Base de données analytique (database.duckdb)")
    print("  [2] SQLite    - Base de données relationnelle (database.sqlite)")
    print("  [3] Commune   - Base de données générique (database.db)")
    print()
    
    while True:
        choice = input("Votre choix (1, 2 ou 3): ").strip()
        if choice in ['1', '2', '3']:
            print()
            return choice
        print("Choix invalide. Veuillez entrer 1, 2 ou 3.")

def get_database_info(choice):
    """Retourne les informations de la base de données selon le choix"""
    db_types = {
        '1': {
            'name': 'DuckDB',
            'file': 'database.duckdb',
            'module': 'duckdb',
            'connector': 'duckdb.connect',
            'port_main': '3000',
            'port_sample': '3001'
        },
        '2': {
            'name': 'SQLite',
            'file': 'database.sqlite',
            'module': 'sqlite3',
            'connector': 'sqlite3.connect',
            'port_main': '4000',
            'port_sample': '4001'
        },
        '3': {
            'name': 'Database Commune',
            'file': 'database.db',
            'module': 'sqlite3',
            'connector': 'sqlite3.connect',
            'port_main': '5000',
            'port_sample': '5001'
        }
    }
    return db_types[choice]

# Nom du projet et structure (sera mise à jour dynamiquement)
PROJECT_NAME = "python-data-build-tool"
DB_INFO = None  # Sera défini au lancement

def get_structure(db_file):
    """Retourne la structure du projet avec le bon fichier de base de données"""
    return {
        "data": ["source", "processed", "final"],
        "db": [db_file],
        "scripts": ["source.py", "transforming.py", "results.py"],
        "macro": ["python-run-process.py", "dagster-run-process.py", "dagster-run-process-launch.py", "dagster-run-process-stop.py", "execution.log"],
        "README.md": None,
        "requirements.txt": None
    }

# Fichier CSV d'exemple
sample_csv_content = """pays,continent,capitale,latitude,longitude,population,salaire_moyen
France,Europe,Paris,48.8566,2.3522,67000000,3200
Allemagne,Europe,Berlin,52.5200,13.4050,83000000,3500
Espagne,Europe,Madrid,40.4168,-3.7038,47000000,2400
Italie,Europe,Rome,41.9028,12.4964,60000000,2800
Royaume-Uni,Europe,Londres,51.5074,-0.1278,67000000,3100
États-Unis,Amérique du Nord,Washington,38.9072,-77.0369,331000000,5500
Canada,Amérique du Nord,Ottawa,45.4215,-75.6972,38000000,4200
Mexique,Amérique du Nord,Mexico,19.4326,-99.1332,126000000,1200
Brésil,Amérique du Sud,Brasilia,-15.8267,-47.9218,213000000,1500
Argentine,Amérique du Sud,Buenos Aires,-34.6037,-58.3816,45000000,1300
Chine,Asie,Pékin,39.9042,116.4074,1400000000,1800
Japon,Asie,Tokyo,35.6762,139.6503,126000000,3800
Inde,Asie,New Delhi,28.6139,77.2090,1380000000,800
Corée du Sud,Asie,Séoul,37.5665,126.9780,52000000,3000
Australie,Océanie,Canberra,-35.2809,149.1300,25000000,4500
Nouvelle-Zélande,Océanie,Wellington,-41.2865,174.7762,5000000,3700
Afrique du Sud,Afrique,Pretoria,-25.7479,28.2293,59000000,1100
Égypte,Afrique,Le Caire,30.0444,31.2357,102000000,600
Nigeria,Afrique,Abuja,9.0765,7.3986,206000000,500
Kenya,Afrique,Nairobi,-1.2864,36.8172,53000000,700
"""


def get_requirements_content(db_info):
    """Retourne le contenu de requirements.txt selon le type de DB"""
    base_requirements = """pandas
dagster
dagster-webserver
"""
    if db_info['module'] == 'duckdb':
        base_requirements += "duckdb\n"
    # sqlite3 est inclus dans Python par défaut
    return base_requirements

def get_dagster_stop_template_main(db_info):
    """Génère le template de stop pour le processus principal avec le bon port"""
    port = db_info['port_main']
    return f'''#!/usr/bin/env python3
"""Script pour arreter le processus Dagster principal (port {port})"""

import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PID_FILE = BASE_DIR / "dagster.pid"

print("Arret du processus Dagster principal (port {port})...")

try:
    # Trouver le PID du processus utilisant le port {port}
    result = subprocess.run(
        ["lsof", "-ti", ":{port}"], 
        capture_output=True, 
        text=True
    )
    
    if result.stdout.strip():
        pids = result.stdout.strip().split('\\n')
        for pid in pids:
            try:
                subprocess.run(["kill", "-9", pid], check=True)
                print(f"✓ Processus {{pid}} arrete (port {port})")
            except subprocess.CalledProcessError:
                print(f"Erreur lors de l arret du processus {{pid}}")
        
        # Supprimer le fichier PID s il existe
        if PID_FILE.exists():
            PID_FILE.unlink()
        
        print("✓ Processus Dagster principal arrete avec succes.")
    else:
        print("Aucun processus Dagster sur le port {port}.")
        # Nettoyer le fichier PID orphelin
        if PID_FILE.exists():
            PID_FILE.unlink()
            
except FileNotFoundError:
    print("Erreur: commande lsof non trouvee.")
    sys.exit(1)
except Exception as e:
    print(f"Erreur inattendue: {{e}}")
    sys.exit(1)
'''

def get_dagster_stop_template_sample(db_info):
    """Génère le template de stop pour le processus sample avec le bon port"""
    port = db_info['port_sample']
    return f'''#!/usr/bin/env python3
"""Script pour arreter le processus Dagster sample (port {port})"""

import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PID_FILE = BASE_DIR / "dagster.pid"

print("Arret du processus Dagster sample (port {port})...")

try:
    # Trouver le PID du processus utilisant le port {port}
    result = subprocess.run(
        ["lsof", "-ti", ":{port}"], 
        capture_output=True, 
        text=True
    )
    
    if result.stdout.strip():
        pids = result.stdout.strip().split('\\n')
        for pid in pids:
            try:
                subprocess.run(["kill", "-9", pid], check=True)
                print(f"✓ Processus {{pid}} arrete (port {port})")
            except subprocess.CalledProcessError:
                print(f"Erreur lors de l arret du processus {{pid}}")
        
        # Supprimer le fichier PID s il existe
        if PID_FILE.exists():
            PID_FILE.unlink()
        
        print("✓ Processus Dagster sample arrete avec succes.")
    else:
        print("Aucun processus Dagster sur le port {port}.")
        # Nettoyer le fichier PID orphelin
        if PID_FILE.exists():
            PID_FILE.unlink()
            
except FileNotFoundError:
    print("Erreur: commande lsof non trouvee.")
    sys.exit(1)
except Exception as e:
    print(f"Erreur inattendue: {{e}}")
    sys.exit(1)
'''

def get_dagster_launch_template_main(db_info):
    """Génère le template de lancement pour le processus principal avec le bon port"""
    port = db_info['port_main']
    return f'''#!/usr/bin/env python3
"""Script pour lancer Dagster avec le fichier d orchestration du projet principal"""

import subprocess
import sys
import os
from pathlib import Path

# Chemin automatique vers le fichier dagster-run-process.py
BASE_DIR = Path(__file__).resolve().parent
DAGSTER_FILE = BASE_DIR / "dagster-run-process.py"
LOG_FILE = BASE_DIR / "dagster.log"
PID_FILE = BASE_DIR / "dagster.pid"

if not DAGSTER_FILE.exists():
    print(f"Erreur: Le fichier {{DAGSTER_FILE}} n existe pas")
    sys.exit(1)

# Verifier si Dagster est deja en cours
if PID_FILE.exists():
    try:
        with open(PID_FILE, "r") as f:
            old_pid = int(f.read().strip())
        # Verifier si le processus existe
        os.kill(old_pid, 0)
        print(f"Dagster est deja en cours d execution (PID: {{old_pid}})")
        print(f"Utilisez dagster-run-process-stop.py pour l arreter d abord.")
        sys.exit(1)
    except (OSError, ValueError):
        # Le processus n existe plus, supprimer le fichier PID
        PID_FILE.unlink()

print(f"Lancement de Dagster avec: {{DAGSTER_FILE}}")
print(f"Interface UI disponible sur: http://localhost:{port}")
print(f"Logs: {{LOG_FILE}}")
print()
print("Dagster sera lance en arriere-plan.")
print("Utilisez dagster-run-process-stop.py pour l arreter.")
print()

try:
    # Lancer en arriere-plan avec nohup
    with open(LOG_FILE, "w") as log:
        process = subprocess.Popen(
            ["nohup", "dagster", "dev", "-f", str(DAGSTER_FILE), "--port", "{port}"],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True
        )
    
    # Sauvegarder le PID
    with open(PID_FILE, "w") as f:
        f.write(str(process.pid))
    
    print(f"✓ Dagster lance avec succes (PID: {{process.pid}})")
    print(f"✓ Vous pouvez fermer ce terminal en toute securite.")
    
except FileNotFoundError:
    print("Erreur: dagster n est pas installe.")
    print("Installez-le avec: pip install dagster dagster-webserver")
    sys.exit(1)
except Exception as e:
    print(f"Erreur lors du lancement: {{e}}")
    sys.exit(1)
'''

def get_dagster_launch_template_sample(db_info):
    """Génère le template de lancement pour le processus sample avec le bon port"""
    port = db_info['port_sample']
    return f'''#!/usr/bin/env python3
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
    print(f"Erreur: Le fichier {{DAGSTER_FILE}} n existe pas")
    sys.exit(1)

# Verifier si Dagster est deja en cours
if PID_FILE.exists():
    try:
        with open(PID_FILE, "r") as f:
            old_pid = int(f.read().strip())
        # Verifier si le processus existe
        os.kill(old_pid, 0)
        print(f"Dagster est deja en cours d execution (PID: {{old_pid}})")
        print(f"Utilisez dagster-run-process-sample-stop.py pour l arreter d abord.")
        sys.exit(1)
    except (OSError, ValueError):
        # Le processus n existe plus, supprimer le fichier PID
        PID_FILE.unlink()

print(f"Lancement de Dagster avec: {{DAGSTER_FILE}}")
print(f"Interface UI disponible sur: http://localhost:{port}")
print(f"Logs: {{LOG_FILE}}")
print()
print("Dagster sera lance en arriere-plan (port {port}).")
print("Utilisez dagster-run-process-sample-stop.py pour l arreter.")
print()

try:
    # Lancer en arriere-plan avec nohup sur le port {port}
    with open(LOG_FILE, "w") as log:
        process = subprocess.Popen(
            ["nohup", "dagster", "dev", "-f", str(DAGSTER_FILE), "--port", "{port}"],
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True
        )
    
    # Sauvegarder le PID
    with open(PID_FILE, "w") as f:
        f.write(str(process.pid))
    
    print(f"✓ Dagster lance avec succes (PID: {{process.pid}})")
    print(f"✓ Vous pouvez fermer ce terminal en toute securite.")
    
except FileNotFoundError:
    print("Erreur: dagster n est pas installe.")
    print("Installez-le avec: pip install dagster dagster-webserver")
    sys.exit(1)
except Exception as e:
    print(f"Erreur lors du lancement: {{e}}")
    sys.exit(1)
'''



# Template de stop pour le processus sample
dagster_stop_template_sample = '''#!/usr/bin/env python3
"""Script pour arreter le processus Dagster sample (port 3001)"""

import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PID_FILE = BASE_DIR / "dagster.pid"

print("Arret du processus Dagster sample (port 3001)...")

try:
    # Trouver le PID du processus utilisant le port 3001
    result = subprocess.run(
        ["lsof", "-ti", ":3001"], 
        capture_output=True, 
        text=True
    )
    
    if result.stdout.strip():
        pids = result.stdout.strip().split('\\n')
        for pid in pids:
            try:
                subprocess.run(["kill", "-9", pid], check=True)
                print(f"✓ Processus {pid} arrete (port 3001)")
            except subprocess.CalledProcessError:
                print(f"Erreur lors de l arret du processus {pid}")
        
        # Supprimer le fichier PID s il existe
        if PID_FILE.exists():
            PID_FILE.unlink()
        
        print("✓ Processus Dagster sample arrete avec succes.")
    else:
        print("Aucun processus Dagster sur le port 3001.")
        # Nettoyer le fichier PID orphelin
        if PID_FILE.exists():
            PID_FILE.unlink()
            
except FileNotFoundError:
    print("Erreur: commande lsof non trouvee.")
    sys.exit(1)
except Exception as e:
    print(f"Erreur inattendue: {e}")
    sys.exit(1)
'''

# Template de lancement Dagster pour le projet principal
dagster_launch_template_main = '''#!/usr/bin/env python3
"""Script pour lancer Dagster avec le fichier d orchestration du projet principal"""

import subprocess
import sys
import os
from pathlib import Path

# Chemin automatique vers le fichier dagster-run-process.py
BASE_DIR = Path(__file__).resolve().parent
DAGSTER_FILE = BASE_DIR / "dagster-run-process.py"
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
print(f"Interface UI disponible sur: http://localhost:3000")
print(f"Logs: {LOG_FILE}")
print()
print("Dagster sera lance en arriere-plan.")
print("Utilisez dagster-run-process-stop.py pour l arreter.")
print()

try:
    # Lancer en arriere-plan avec nohup
    with open(LOG_FILE, "w") as log:
        process = subprocess.Popen(
            ["nohup", "dagster", "dev", "-f", str(DAGSTER_FILE)],
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
'''

# Template de lancement Dagster pour le sample
dagster_launch_template_sample = '''#!/usr/bin/env python3
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
        print(f"Utilisez dagster-run-process-sample-stop.py pour l arreter d abord.")
        sys.exit(1)
    except (OSError, ValueError):
        # Le processus n existe plus, supprimer le fichier PID
        PID_FILE.unlink()

print(f"Lancement de Dagster avec: {DAGSTER_FILE}")
print(f"Interface UI disponible sur: http://localhost:3001")
print(f"Logs: {LOG_FILE}")
print()
print("Dagster sera lance en arriere-plan (port 3001).")
print("Utilisez dagster-run-process-sample-stop.py pour l arreter.")
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
'''

# Template Dagster pour le projet principal
dagster_template_main = '''"""
Orchestration Dagster pour le pipeline python-data-build-tool principal
Lance le processus toutes les 5 minutes
"""

from pathlib import Path
import sys
from datetime import datetime

from dagster import (
    asset,
    define_asset_job,
    ScheduleDefinition,
    Definitions,
    AssetExecutionContext,
)

# Chemin de base - Le fichier est dans macro/, on remonte au parent
BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = BASE_DIR / "scripts"


@asset(group_name="main_pipeline")
def import_data(context: AssetExecutionContext) -> dict:
    """Import des données sources vers la base de données"""
    context.log.info("Début de l'import des données")
    
    sys.path.insert(0, str(SCRIPTS_DIR))
    
    try:
        from source import import_data as do_import
        do_import()
        context.log.info("Import terminé avec succès")
        return {"status": "success", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        context.log.error(f"Erreur lors de l'import: {e}")
        raise
    finally:
        if str(SCRIPTS_DIR) in sys.path:
            sys.path.remove(str(SCRIPTS_DIR))


@asset(group_name="main_pipeline", deps=[import_data])
def transform_data(context: AssetExecutionContext) -> dict:
    """Transformation des données"""
    context.log.info("Début de la transformation")
    
    sys.path.insert(0, str(SCRIPTS_DIR))
    
    try:
        from transforming import transform_data as do_transform
        do_transform()
        context.log.info("Transformation terminée avec succès")
        return {"status": "success", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        context.log.error(f"Erreur lors de la transformation: {e}")
        raise
    finally:
        if str(SCRIPTS_DIR) in sys.path:
            sys.path.remove(str(SCRIPTS_DIR))


@asset(group_name="main_pipeline", deps=[transform_data])
def export_results(context: AssetExecutionContext) -> dict:
    """Export des résultats finaux"""
    context.log.info("Début de l'export")
    
    sys.path.insert(0, str(SCRIPTS_DIR))
    
    try:
        from results import export_results as do_export
        do_export()
        context.log.info("Export terminé avec succès")
        return {"status": "success", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        context.log.error(f"Erreur lors de l'export: {e}")
        raise
    finally:
        if str(SCRIPTS_DIR) in sys.path:
            sys.path.remove(str(SCRIPTS_DIR))


# Job principal
main_job = define_asset_job(
    name="main_pipeline_job",
    selection=[import_data, transform_data, export_results],
    description="Pipeline complet ETL principal"
)

# Schedule toutes les 5 minutes
main_schedule = ScheduleDefinition(
    name="main_every_5min",
    job=main_job,
    cron_schedule="*/5 * * * *",
    description="Exécute le pipeline toutes les 5 minutes"
)

# Définitions Dagster
defs = Definitions(
    assets=[import_data, transform_data, export_results],
    jobs=[main_job],
    schedules=[main_schedule],
)


if __name__ == "__main__":
    print("=" * 60)
    print("DAGSTER - Pipeline Principal")
    print("=" * 60)
    print()
    print("Lancer l'interface web:")
    print("  dagster dev -f dagster-run-process.py")
    print()
    print("UI: http://localhost:3000")
    print()
    print("Job: main_pipeline_job")
    print("Schedule: main_every_5min (toutes les 5 minutes)")
    print()
    print("=" * 60)
'''

# Template Dagster pour le sample
dagster_template_sample_file = '''"""
Orchestration Dagster pour le pipeline python-data-build-tool sample
Lance le processus toutes les 5 minutes
"""

from pathlib import Path
import sys
from datetime import datetime

from dagster import (
    asset,
    define_asset_job,
    ScheduleDefinition,
    Definitions,
    AssetExecutionContext,
)

# Chemin de base - Le fichier est dans sample/macro/, on remonte de 2 niveaux
BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = BASE_DIR / "scripts"


@asset(group_name="sample_pipeline")
def sample_import_data(context: AssetExecutionContext) -> dict:
    """Import des données sources vers la base de données (sample)"""
    context.log.info("Début de l'import des données (sample)")
    
    sys.path.insert(0, str(SCRIPTS_DIR))
    
    try:
        from source import import_data
        import_data()
        context.log.info("Import terminé avec succès (sample)")
        return {"status": "success", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        context.log.error(f"Erreur lors de l'import (sample): {e}")
        raise
    finally:
        if str(SCRIPTS_DIR) in sys.path:
            sys.path.remove(str(SCRIPTS_DIR))


@asset(group_name="sample_pipeline", deps=[sample_import_data])
def sample_transform_data(context: AssetExecutionContext) -> dict:
    """Transformation des données (sample)"""
    context.log.info("Début de la transformation (sample)")
    
    sys.path.insert(0, str(SCRIPTS_DIR))
    
    try:
        from transforming import transform_data
        transform_data()
        context.log.info("Transformation terminée avec succès (sample)")
        return {"status": "success", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        context.log.error(f"Erreur lors de la transformation (sample): {e}")
        raise
    finally:
        if str(SCRIPTS_DIR) in sys.path:
            sys.path.remove(str(SCRIPTS_DIR))


@asset(group_name="sample_pipeline", deps=[sample_transform_data])
def sample_export_results(context: AssetExecutionContext) -> dict:
    """Export des résultats finaux (sample)"""
    context.log.info("Début de l'export (sample)")
    
    sys.path.insert(0, str(SCRIPTS_DIR))
    
    try:
        from results import export_results
        export_results()
        context.log.info("Export terminé avec succès (sample)")
        return {"status": "success", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        context.log.error(f"Erreur lors de l'export (sample): {e}")
        raise
    finally:
        if str(SCRIPTS_DIR) in sys.path:
            sys.path.remove(str(SCRIPTS_DIR))


# Job sample
sample_job = define_asset_job(
    name="sample_pipeline_job",
    selection=[sample_import_data, sample_transform_data, sample_export_results],
    description="Pipeline complet ETL sample"
)

# Schedule toutes les 5 minutes
sample_schedule = ScheduleDefinition(
    name="sample_every_5min",
    job=sample_job,
    cron_schedule="*/5 * * * *",
    description="Exécute le pipeline sample toutes les 5 minutes"
)

# Définitions Dagster
defs = Definitions(
    assets=[sample_import_data, sample_transform_data, sample_export_results],
    jobs=[sample_job],
    schedules=[sample_schedule],
)


if __name__ == "__main__":
    print("=" * 60)
    print("DAGSTER - Pipeline Sample")
    print("=" * 60)
    print()
    print("Lancer l'interface web:")
    print("  dagster dev -f dagster-run-process-sample.py --port 3001")
    print()
    print("UI: http://localhost:3001")
    print()
    print("Job: sample_pipeline_job")
    print("Schedule: sample_every_5min (toutes les 5 minutes)")
    print()
    print("=" * 60)
'''

def get_readme_content(db_info):
    """Génère le README avec les bonnes informations de base de données"""
    return f"""
# Python DuckDB - Pipeline de données orchestré

Ce projet permet de gérer un flux complet de données depuis l'import de fichiers sources.
Il inclut la transformation et l'agrégation des données pour obtenir des résultats prêts à l'analyse.
Les résultats finaux peuvent être exportés pour usage externe.

## Configuration de la base de données

**Type de base de données:** {db_info['name']}
**Fichier:** {db_info['file']}
**Module:** {db_info['module']}

## Démarrage rapide

### Exécution classique (manuelle)
```bash
cd macro/
python3 python-run-process.py
```

### Orchestration avec Dagster
```bash
# Lancer l'interface web principale (ports selon DB: 3000 DuckDB, 4000 SQLite, 5000 Commune)
cd macro/
python3 dagster-run-process-launch.py

# Lancer l'exemple sample (port 3001)
cd sample/macro/
python3 dagster-run-process-sample-launch.py

# Arrêter Dagster
# Pour le principal:
python3 dagster-run-process-stop.py

# Pour le sample:
python3 dagster-run-process-sample-stop.py
```

**Interfaces web selon le type de base de données:**
- **DuckDB**: Principal (http://localhost:3000), Sample (http://localhost:3001)
- **SQLite**: Principal (http://localhost:4000), Sample (http://localhost:4001)
- **Database Commune**: Principal (http://localhost:5000), Sample (http://localhost:5001)

## Architecture du flux de données

```
    CSV Files                  Database                    CSV Files
    (Source)                 (Processing)                  (Output)
        │                          │                           │
        │                          │                           │
        ▼                          ▼                           ▼
  ┌─────────┐              ┌──────────────┐              ┌──────────┐
  │ data/   │   source.py  │   Database   │  results.py  │  data/   │
  │ source/ │─────────────▶│              │─────────────▶│processed/│
  │         │   (import)   │   Tables:    │   (export)   │  final/  │
  │ *.csv   │              │   - SOURCE   │              │  *.csv   │
  └─────────┘              │   - TRANS.   │              └──────────┘
                           │   - SUMMARY  │
                           └──────────────┘
                                   │
                                   │ transforming.py
                                   │ (transform & aggregate)
                                   ▼
                           ┌──────────────┐
                           │    Python    │
                           │   + Pandas   │
                           │   + {db_info['module']:8} │
                           │   (Engine)   │
                           └──────────────┘
```

## Structure du projet

```
project/
├── data/                   
│   ├── source/             Données brutes importées depuis CSV ou autres sources
│   ├── processed/          Données transformées prêtes à être utilisées
│   └── final/              Données finales ou résultats finaux
│
├── db/                     
│   └── {db_info['file']}     Base de données principale du projet
│
├── scripts/                
│   ├── source.py           Script d'import des données depuis les fichiers sources
│   ├── transforming.py     Script pour transformer et agréger les données
│   └── results.py          Script pour générer et exporter les résultats finaux
│
├── macro/                  Scripts d'orchestration
│   ├── python-run-process.py              Exécution classique du pipeline
│   ├── dagster-run-process.py             Définition Dagster du pipeline
│   ├── dagster-run-process-launch.py      Lancement de Dagster (port selon DB)
│   ├── dagster-run-process-stop.py        Arrêt du processus principal
│   ├── dagster.log                        Logs Dagster
│   ├── dagster.pid                        PID du processus Dagster
│   └── execution.log                      Logs d'exécution du pipeline
│
├── sample/                 Dossier contenant un exemple complet avec données de test
│   ├── data/
│   │   └── source/
│   │       └── SAMPLE_DATA.csv           Données d'exemple
│   ├── db/
│   │   └── {db_info['file']}
│   ├── scripts/            Scripts fonctionnels pour l'exemple
│   └── macro/
│       ├── python-run-process-sample.py
│       ├── dagster-run-process-sample.py
│       └── dagster-run-process-sample-launch.py (port 3001)
│
├── requirements.txt        Dépendances: pandas{', duckdb' if db_info['module'] == 'duckdb' else ''}, dagster, dagster-webserver
├── python-data-build-tool.py        Générateur de structure de projet
└── README.md               Cette documentation
```

## Modes d'exécution

### 1. Mode classique (python-run-process.py)
Exécution directe et unique du pipeline:
- Import des données
- Transformation
- Export des résultats
- Logs dans `execution.log`

### 2. Mode Dagster (orchestration)
Exécution orchestrée avec interface web:
- **Visualisation** du pipeline sous forme de graphe
- **Scheduling** automatique toutes les 5 minutes
- **Monitoring** en temps réel
- **Historique** des exécutions
- **Logs** centralisés

#### Avantages de Dagster:
- Interface graphique intuitive
- Gestion des dépendances entre assets
- Retry automatique en cas d'erreur
- Métriques et observabilité
- Exécution en arrière-plan

## Scripts utilitaires

### dagster-run-process-launch.py
Lance Dagster en arrière-plan avec:
- Détection de processus existant (évite les doublons)
- Gestion du PID dans `dagster.pid`
- Logs redirigés vers `dagster.log`
- Survit à la fermeture du terminal

### dagster-run-process-stop.py / dagster-run-process-sample-stop.py
Arrête le processus Dagster spécifique:
- `dagster-run-process-stop.py`: Arrête le processus principal (port selon DB)
- `dagster-run-process-sample-stop.py`: Arrête le processus sample (port selon DB)
- Kill du processus sur le port concerné uniquement
- Nettoyage des fichiers PID
- Vérification de l'arrêt complet

**Ports par type de base de données:**
- DuckDB: 3000 (principal), 3001 (sample)
- SQLite: 4000 (principal), 4001 (sample)  
- Database Commune: 5000 (principal), 5001 (sample)

## Installation

```bash
pip install -r requirements.txt
```

Dépendances:
- `pandas` - Manipulation de données
{f"- `duckdb` - Base de données analytique" if db_info['module'] == 'duckdb' else ''}
- `dagster` - Framework d'orchestration
- `dagster-webserver` - Interface web Dagster

## Exemple avec données de test

Le dossier `sample/` contient un exemple complet fonctionnel:

```bash
cd sample/macro/

# Exécution classique
python3 python-run-process-sample.py

# Avec Dagster (port 3001)
python3 dagster-run-process-sample-launch.py
```

Données d'exemple: `sample/data/source/SAMPLE_DATA.csv`

## Ports réseau par type de base de données

**DuckDB:**
- Principal: Port 3000 (http://localhost:3000)
- Sample: Port 3001 (http://localhost:3001)

**SQLite:**
- Principal: Port 4000 (http://localhost:4000)
- Sample: Port 4001 (http://localhost:4001)

**Database Commune:**
- Principal: Port 5000 (http://localhost:5000)
- Sample: Port 5001 (http://localhost:5001)

Les six instances peuvent fonctionner simultanément sans conflit.

## Logs

- `macro/execution.log` - Logs du pipeline (import, transformation, export)
- `macro/dagster.log` - Logs du serveur Dagster
- `macro/dagster.pid` - PID du processus Dagster actif

"""


def get_python_templates(db_info):
    """Génère les templates Python adaptés au type de base de données"""
    
    if db_info['module'] == 'duckdb':
        # Templates DuckDB
        return {
            "source.py": textwrap.dedent("""
                from pathlib import Path
                import pandas as pd
                import duckdb

                def import_data():
                    pass
                """),
            
            "transforming.py": textwrap.dedent("""
                from pathlib import Path
                import pandas as pd
                import duckdb

                def transform_data():
                    pass
                """),
            
            "results.py": textwrap.dedent("""
                from pathlib import Path
                import pandas as pd
                import duckdb
            
                def export_results():
                    pass
                """),
            
            "python-run-process.py": textwrap.dedent("""
                from datetime import datetime
                from pathlib import Path
                import logging
                import sys

                BASE_DIR = Path(__file__).resolve().parent.parent
                SCRIPTS_DIR = BASE_DIR / "scripts"
                sys.path.insert(0, str(SCRIPTS_DIR))

                from source import import_data  # type: ignore
                from transforming import transform_data  # type: ignore
                from results import export_results  # type: ignore

                LOG_PATH = BASE_DIR / "macro" / "execution.log"

                logging.basicConfig(
                    filename=str(LOG_PATH),
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s"
                )

                logger = logging.getLogger(__name__)

                def run_process():
                    start_time = datetime.now()

                    with open(LOG_PATH, "a") as f:
                        f.write("\\n")

                    logger.info("Début du process")

                    import_data()
                    transform_data()
                    export_results()

                    logger.info("Fin du process")
                    duration = datetime.now() - start_time
                    logger.info(f"Durée du process : {duration}")

                    with open(LOG_PATH, "a") as f:
                        f.write("\\n")

                    print(f"Process terminé {duration}")

                if __name__ == "__main__":
                    run_process()
                """),
        }
    else:
        # Templates SQLite
        return {
            "source.py": textwrap.dedent("""
                from pathlib import Path
                import pandas as pd
                import sqlite3

                def import_data():
                    pass
                """),
            
            "transforming.py": textwrap.dedent("""
                from pathlib import Path
                import pandas as pd
                import sqlite3

                def transform_data():
                    pass
                """),
            
            "results.py": textwrap.dedent("""
                from pathlib import Path
                import pandas as pd
                import sqlite3
            
                def export_results():
                    pass
                """),
            
            "python-run-process.py": textwrap.dedent("""
                from datetime import datetime
                from pathlib import Path
                import logging
                import sys

                BASE_DIR = Path(__file__).resolve().parent.parent
                SCRIPTS_DIR = BASE_DIR / "scripts"
                sys.path.insert(0, str(SCRIPTS_DIR))

                from source import import_data  # type: ignore
                from transforming import transform_data  # type: ignore
                from results import export_results  # type: ignore

                LOG_PATH = BASE_DIR / "macro" / "execution.log"

                logging.basicConfig(
                    filename=str(LOG_PATH),
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s"
                )

                logger = logging.getLogger(__name__)

                def run_process():
                    start_time = datetime.now()

                    with open(LOG_PATH, "a") as f:
                        f.write("\\n")

                    logger.info("Début du process")

                    import_data()
                    transform_data()
                    export_results()

                    logger.info("Fin du process")
                    duration = datetime.now() - start_time
                    logger.info(f"Durée du process : {duration}")

                    with open(LOG_PATH, "a") as f:
                        f.write("\\n")

                    print(f"Process terminé {duration}")

                if __name__ == "__main__":
                    run_process()
                """),
        }


def get_python_templates_sample(db_info):
    """Génère les templates Python pour le sample selon le type de DB"""
    
    if db_info['module'] == 'duckdb':
        # Templates DuckDB pour le sample
        return {
            "python-run-process-sample.py": textwrap.dedent("""
                from datetime import datetime
                from pathlib import Path
                import logging
                import sys

                BASE_DIR = Path(__file__).resolve().parent.parent
                SCRIPTS_DIR = BASE_DIR / "scripts"
                sys.path.insert(0, str(SCRIPTS_DIR))

                from source import import_data  # type: ignore
                from transforming import transform_data  # type: ignore
                from results import export_results  # type: ignore

                LOG_PATH = BASE_DIR / "macro" / "execution.log"

                logging.basicConfig(
                    filename=str(LOG_PATH),
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s"
                )

                logger = logging.getLogger(__name__)

                def run_process():
                    start_time = datetime.now()

                    with open(LOG_PATH, "a") as f:
                        f.write("\\n")

                    logger.info("Début du process")

                    import_data()
                    transform_data()
                    export_results()

                    logger.info("Fin du process")
                    duration = datetime.now() - start_time
                    logger.info(f"Durée du process : {duration}")

                    with open(LOG_PATH, "a") as f:
                        f.write("\\n")

                    print(f"Process terminé {duration}")

                if __name__ == "__main__":
                    run_process()
                """),
            
            "source.py": textwrap.dedent(f"""
                from pathlib import Path
                import pandas as pd
                import duckdb

                BASE_DIR = Path(__file__).resolve().parent.parent
                DB_PATH = BASE_DIR / "db" / "{db_info['file']}"
                SOURCE_DIR = BASE_DIR / "data" / "source"

                def import_data():
                    '''Importe les données CSV vers la base de données DuckDB'''
                    
                    # Connexion à la base de données
                    conn = duckdb.connect(str(DB_PATH))
                    
                    # Parcourir tous les fichiers CSV dans data/source/
                    csv_files = list(SOURCE_DIR.glob("*.csv"))
                    
                    if not csv_files:
                        print("Aucun fichier CSV trouvé dans data/source/")
                        conn.close()
                        return
                    
                    for csv_file in csv_files:
                        # Nom de la table = nom du fichier sans extension
                        table_name = csv_file.stem.upper()
                        
                        # Import direct depuis CSV avec DuckDB
                        conn.execute(f"CREATE OR REPLACE TABLE {{table_name}} AS SELECT * FROM read_csv_auto('{{csv_file}}')")
                        
                        # Compter les lignes
                        count = conn.execute(f"SELECT COUNT(*) FROM {{table_name}}").fetchone()[0]
                        
                        print(f"✓ {{csv_file.name}} importé dans la table {{table_name}} ({{count}} lignes)")
                    
                    conn.close()
                    print(f"Import terminé: {{len(csv_files)}} fichier(s) traité(s)")
                
                if __name__ == "__main__":
                    import_data()
                """),
            
            "transforming.py": textwrap.dedent(f"""
                from pathlib import Path
                import pandas as pd
                import duckdb

                BASE_DIR = Path(__file__).resolve().parent.parent
                DB_PATH = BASE_DIR / "db" / "{db_info['file']}"

                def transform_data():
                    '''Transforme les données sources et crée TRANSFORMED_DATA et SUMMARY_DATA dans la base'''
                    
                    # Connexion à la base de données
                    conn = duckdb.connect(str(DB_PATH))
                    
                    try:
                        # Transformation 1: Créer TRANSFORMED_DATA avec calculs supplémentaires
                        conn.execute(\"\"\"
                            CREATE OR REPLACE TABLE TRANSFORMED_DATA AS
                            SELECT 
                                *,
                                ROUND(population / 1000000.0, 2) AS population_millions,
                                salaire_moyen * 12 AS salaire_annuel
                            FROM SAMPLE_DATA
                        \"\"\")
                        
                        count = conn.execute("SELECT COUNT(*) FROM TRANSFORMED_DATA").fetchone()[0]
                        print(f"✓ Table TRANSFORMED_DATA créée ({{count}} lignes)")
                        
                        # Transformation 2: Créer SUMMARY_DATA (agrégation par continent)
                        conn.execute(\"\"\"
                            CREATE OR REPLACE TABLE SUMMARY_DATA AS
                            SELECT 
                                continent,
                                ROUND(SUM(population) / 1000000.0, 2) AS population_totale,
                                CAST(ROUND(AVG(salaire_moyen), 0) AS INTEGER) AS salaire_moyen,
                                COUNT(*) AS nombre_pays
                            FROM SAMPLE_DATA
                            GROUP BY continent
                            ORDER BY continent
                        \"\"\")
                        
                        count = conn.execute("SELECT COUNT(*) FROM SUMMARY_DATA").fetchone()[0]
                        print(f"✓ Table SUMMARY_DATA créée ({{count}} continents)")
                        
                    except Exception as e:
                        print(f"Erreur lors de la transformation: {{e}}")
                    
                    conn.close()
                
                if __name__ == "__main__":
                    transform_data()
                """),
            
            "results.py": textwrap.dedent(f"""
                from pathlib import Path
                import pandas as pd
                import duckdb
            
                BASE_DIR = Path(__file__).resolve().parent.parent
                DB_PATH = BASE_DIR / "db" / "{db_info['file']}"
                PROCESSED_DIR = BASE_DIR / "data" / "processed"
                FINAL_DIR = BASE_DIR / "data" / "final"

                def export_results():
                    '''Exporte TRANSFORMED_DATA vers processed/ et SUMMARY_DATA vers final/'''
                    
                    # Connexion à la base de données
                    conn = duckdb.connect(str(DB_PATH))
                    
                    try:
                        # Exporter TRANSFORMED_DATA vers data/processed/
                        output_processed = PROCESSED_DIR / "TRANSFORMED_DATA.csv"
                        conn.execute(f"COPY TRANSFORMED_DATA TO '{{output_processed}}' (HEADER, DELIMITER ',')")
                        
                        count = conn.execute("SELECT COUNT(*) FROM TRANSFORMED_DATA").fetchone()[0]
                        print(f"✓ TRANSFORMED_DATA exporté vers processed/ ({{count}} lignes)")
                        
                        # Exporter SUMMARY_DATA vers data/final/
                        output_final = FINAL_DIR / "SUMMARY_DATA.csv"
                        conn.execute(f"COPY SUMMARY_DATA TO '{{output_final}}' (HEADER, DELIMITER ',')")
                        
                        count = conn.execute("SELECT COUNT(*) FROM SUMMARY_DATA").fetchone()[0]
                        print(f"✓ SUMMARY_DATA exporté vers final/ ({{count}} lignes)")
                        
                    except Exception as e:
                        print(f"Erreur lors de l'export: {{e}}")
                    
                    conn.close()
                
                if __name__ == "__main__":
                    export_results()
                """),
        }
    else:
        # Templates SQLite pour le sample
        return {
            "python-run-process-sample.py": textwrap.dedent("""
                from datetime import datetime
                from pathlib import Path
                import logging
                import sys

                BASE_DIR = Path(__file__).resolve().parent.parent
                SCRIPTS_DIR = BASE_DIR / "scripts"
                sys.path.insert(0, str(SCRIPTS_DIR))

                from source import import_data  # type: ignore
                from transforming import transform_data  # type: ignore
                from results import export_results  # type: ignore

                LOG_PATH = BASE_DIR / "macro" / "execution.log"

                logging.basicConfig(
                    filename=str(LOG_PATH),
                    level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s"
                )

                logger = logging.getLogger(__name__)

                def run_process():
                    start_time = datetime.now()

                    with open(LOG_PATH, "a") as f:
                        f.write("\\n")

                    logger.info("Début du process")

                    import_data()
                    transform_data()
                    export_results()

                    logger.info("Fin du process")
                    duration = datetime.now() - start_time
                    logger.info(f"Durée du process : {duration}")

                    with open(LOG_PATH, "a") as f:
                        f.write("\\n")

                    print(f"Process terminé {duration}")

                if __name__ == "__main__":
                    run_process()
                """),
            
            "source.py": textwrap.dedent(f"""
                from pathlib import Path
                import pandas as pd
                import sqlite3

                BASE_DIR = Path(__file__).resolve().parent.parent
                DB_PATH = BASE_DIR / "db" / "{db_info['file']}"
                SOURCE_DIR = BASE_DIR / "data" / "source"

                def import_data():
                    '''Importe les données CSV vers la base de données SQLite'''
                    
                    # Connexion à la base de données
                    conn = sqlite3.connect(str(DB_PATH))
                    
                    # Parcourir tous les fichiers CSV dans data/source/
                    csv_files = list(SOURCE_DIR.glob("*.csv"))
                    
                    if not csv_files:
                        print("Aucun fichier CSV trouvé dans data/source/")
                        conn.close()
                        return
                    
                    for csv_file in csv_files:
                        # Nom de la table = nom du fichier sans extension
                        table_name = csv_file.stem.upper()
                        
                        # Lire le CSV avec pandas
                        df = pd.read_csv(csv_file)
                        
                        # Import vers SQLite avec pandas
                        df.to_sql(table_name, conn, if_exists='replace', index=False)
                        
                        # Compter les lignes
                        cursor = conn.cursor()
                        cursor.execute(f"SELECT COUNT(*) FROM {{table_name}}")
                        count = cursor.fetchone()[0]
                        
                        print(f"✓ {{csv_file.name}} importé dans la table {{table_name}} ({{count}} lignes)")
                    
                    conn.close()
                    print(f"Import terminé: {{len(csv_files)}} fichier(s) traité(s)")
                
                if __name__ == "__main__":
                    import_data()
                """),
            
            "transforming.py": textwrap.dedent(f"""
                from pathlib import Path
                import pandas as pd
                import sqlite3

                BASE_DIR = Path(__file__).resolve().parent.parent
                DB_PATH = BASE_DIR / "db" / "{db_info['file']}"

                def transform_data():
                    '''Transforme les données sources et crée TRANSFORMED_DATA et SUMMARY_DATA dans la base'''
                    
                    # Connexion à la base de données
                    conn = sqlite3.connect(str(DB_PATH))
                    cursor = conn.cursor()
                    
                    try:
                        # Transformation 1: Créer TRANSFORMED_DATA avec calculs supplémentaires
                        cursor.execute("DROP TABLE IF EXISTS TRANSFORMED_DATA")
                        cursor.execute(\"\"\"
                            CREATE TABLE TRANSFORMED_DATA AS
                            SELECT 
                                *,
                                ROUND(population / 1000000.0, 2) AS population_millions,
                                salaire_moyen * 12 AS salaire_annuel
                            FROM SAMPLE_DATA
                        \"\"\")
                        conn.commit()
                        
                        cursor.execute("SELECT COUNT(*) FROM TRANSFORMED_DATA")
                        count = cursor.fetchone()[0]
                        print(f"✓ Table TRANSFORMED_DATA créée ({{count}} lignes)")
                        
                        # Transformation 2: Créer SUMMARY_DATA (agrégation par continent)
                        cursor.execute("DROP TABLE IF EXISTS SUMMARY_DATA")
                        cursor.execute(\"\"\"
                            CREATE TABLE SUMMARY_DATA AS
                            SELECT 
                                continent,
                                ROUND(SUM(population) / 1000000.0, 2) AS population_totale,
                                CAST(ROUND(AVG(salaire_moyen), 0) AS INTEGER) AS salaire_moyen,
                                COUNT(*) AS nombre_pays
                            FROM SAMPLE_DATA
                            GROUP BY continent
                            ORDER BY continent
                        \"\"\")
                        conn.commit()
                        
                        cursor.execute("SELECT COUNT(*) FROM SUMMARY_DATA")
                        count = cursor.fetchone()[0]
                        print(f"✓ Table SUMMARY_DATA créée ({{count}} continents)")
                        
                    except Exception as e:
                        print(f"Erreur lors de la transformation: {{e}}")
                    
                    conn.close()
                
                if __name__ == "__main__":
                    transform_data()
                """),
            
            "results.py": textwrap.dedent(f"""
                from pathlib import Path
                import pandas as pd
                import sqlite3
            
                BASE_DIR = Path(__file__).resolve().parent.parent
                DB_PATH = BASE_DIR / "db" / "{db_info['file']}"
                PROCESSED_DIR = BASE_DIR / "data" / "processed"
                FINAL_DIR = BASE_DIR / "data" / "final"

                def export_results():
                    '''Exporte TRANSFORMED_DATA vers processed/ et SUMMARY_DATA vers final/'''
                    
                    # Connexion à la base de données
                    conn = sqlite3.connect(str(DB_PATH))
                    
                    try:
                        # Exporter TRANSFORMED_DATA vers data/processed/
                        output_processed = PROCESSED_DIR / "TRANSFORMED_DATA.csv"
                        df_transformed = pd.read_sql_query("SELECT * FROM TRANSFORMED_DATA", conn)
                        df_transformed.to_csv(output_processed, index=False)
                        
                        print(f"✓ TRANSFORMED_DATA exporté vers processed/ ({{len(df_transformed)}} lignes)")
                        
                        # Exporter SUMMARY_DATA vers data/final/
                        output_final = FINAL_DIR / "SUMMARY_DATA.csv"
                        df_summary = pd.read_sql_query("SELECT * FROM SUMMARY_DATA", conn)
                        df_summary.to_csv(output_final, index=False)
                        
                        print(f"✓ SUMMARY_DATA exporté vers final/ ({{len(df_summary)}} lignes)")
                        
                    except Exception as e:
                        print(f"Erreur lors de l'export: {{e}}")
                    
                    conn.close()
                
                if __name__ == "__main__":
                    export_results()
                """),
        }


# Structure pour le sample (différente pour le fichier macro)
def get_structure_sample(db_file):
    return {
        "data": ["source", "processed", "final"],
        "db": [db_file],
        "scripts": ["source.py", "transforming.py", "results.py"],
        "macro": ["python-run-process-sample.py", "dagster-run-process-sample.py", "dagster-run-process-sample-launch.py", "dagster-run-process-sample-stop.py", "execution.log"],
        "README.md": None,
        "requirements.txt": None
    }

# Fonction pour créer la structure du projet
def create_structure(base_path, struct, db_info, with_sample=False):
    # Choisir le bon template selon si c'est un sample ou non
    templates = get_python_templates_sample(db_info) if with_sample else get_python_templates(db_info)
    
    for key, value in struct.items():
        if isinstance(value, list):
            dir_path = base_path / key
            dir_path.mkdir(parents=True, exist_ok=True)
            
            # Ajouter .gitkeep dans tous les dossiers
            gitkeep_path = dir_path / ".gitkeep"
            gitkeep_path.touch()
            
            for item in value:
                item_path = dir_path / item
                if "." in item:
                    # Créer le fichier de base de données avec le bon module
                    if item == db_info['file']:
                        if db_info['module'] == 'duckdb':
                            import duckdb
                            conn = duckdb.connect(str(item_path))
                            conn.close()
                        else:
                            import sqlite3
                            conn = sqlite3.connect(str(item_path))
                            conn.close()
                    else:
                        item_path.touch()
                        if item in templates:
                            item_path.write_text(templates[item], encoding='utf-8')
                    # Créer les fichiers Dagster dans macro/
                    if item == "dagster-run-process.py":
                        dagster_content = dagster_template_main
                        item_path.write_text(dagster_content, encoding='utf-8')
                    elif item == "dagster-run-process-sample.py":
                        dagster_content = dagster_template_sample_file
                        item_path.write_text(dagster_content, encoding='utf-8')
                    elif item == "dagster-run-process-launch.py":
                        # Template de lancement pour le projet principal
                        item_path.write_text(get_dagster_launch_template_main(db_info), encoding='utf-8')
                        # Rendre le fichier exécutable
                        item_path.chmod(0o755)
                    elif item == "dagster-run-process-sample-launch.py":
                        # Template de lancement pour le sample
                        item_path.write_text(get_dagster_launch_template_sample(db_info), encoding='utf-8')
                        # Rendre le fichier exécutable
                        item_path.chmod(0o755)
                    elif item == "dagster-run-process-stop.py":
                        # Template pour arrêter Dagster principal
                        item_path.write_text(get_dagster_stop_template_main(db_info), encoding='utf-8')
                        # Rendre le fichier exécutable
                        item_path.chmod(0o755)
                    elif item == "dagster-run-process-sample-stop.py":
                        # Template pour arrêter Dagster sample
                        item_path.write_text(get_dagster_stop_template_sample(db_info), encoding='utf-8')
                        # Rendre le fichier exécutable
                        item_path.chmod(0o755)
                else:
                    item_path.mkdir(exist_ok=True)
                    # Ajouter .gitkeep dans les sous-dossiers
                    sub_gitkeep = item_path / ".gitkeep"
                    sub_gitkeep.touch()
        else:
            file_path = base_path / key
            file_path.touch()
            if key == "README.md":
                file_path.write_text(get_readme_content(db_info), encoding='utf-8')
            elif key == "requirements.txt":
                file_path.write_text(get_requirements_content(db_info), encoding='utf-8')
    
    # Créer le fichier CSV d'exemple uniquement si avec sample
    if with_sample:
        sample_csv_path = base_path / "data" / "source" / "SAMPLE_DATA.csv"
        sample_csv_path.write_text(sample_csv_content, encoding='utf-8')
    
    # Parcourir tous les dossiers créés et ajouter .gitkeep partout
    for item in base_path.rglob('*'):
        if item.is_dir():
            gitkeep_path = item / ".gitkeep"
            gitkeep_path.touch()

# Création du projet
if __name__ == "__main__":
    # Choix du type de base de données
    db_choice = choose_database_type()
    db_info = get_database_info(db_choice)
    
    print(f"Base de données sélectionnée: {db_info['name']} ({db_info['file']})")
    print()
    
    # Vérifier et installer les dépendances
    check_and_install_dependencies(db_info)
    
    base_path = Path.cwd() / PROJECT_NAME
    base_path.mkdir(exist_ok=True)
    
    # Créer la structure principale (sans exemple)
    structure = get_structure(db_info['file'])
    create_structure(base_path, structure, db_info, with_sample=False)
    
    # Créer le sous-dossier sample avec l'exemple
    sample_path = base_path / "sample"
    sample_path.mkdir(exist_ok=True)
    structure_sample = get_structure_sample(db_info['file'])
    create_structure(sample_path, structure_sample, db_info, with_sample=True)
    
    # Copier le script de génération
    current_file = Path(__file__).resolve()
    shutil.copy(current_file, base_path / "python-data-build-tool.py")
    
    print(f"{PROJECT_NAME} créé avec succès dans {base_path}")
    print(f"✓ Structure principale créée")
    print(f"✓ Exemple disponible dans {PROJECT_NAME}/sample/")
    print(f"✓ Type de base de données: {db_info['name']}")
