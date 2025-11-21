import os
from pathlib import Path
import shutil
import textwrap


# Nom du projet et structure
PROJECT_NAME = "python-duckdb"

structure = {
    "data": ["source", "processed", "final"],
    "db": ["database.duckdb"],
    "scripts": ["source.py", "transforming.py", "results.py"],
    "macro": ["python-run-process.py", "dagster-run-process.py", "dagster-run-process-launch.py", "dagster-stop.py", "execution.log"],
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


requirements_content = """pandas
duckdb
"""

# Template pour arrêter Dagster
dagster_stop_template = '''#!/usr/bin/env python3
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
print("Utilisez dagster-stop.py pour l arreter.")
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
'''

# Template Dagster pour le projet principal
dagster_template_main = '''"""
Orchestration Dagster pour le pipeline python-duckdb principal
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

# Chemin de base
BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "scripts"


@asset(group_name="main_pipeline")
def import_data(context: AssetExecutionContext) -> dict:
    """Import des données sources vers DuckDB"""
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
Orchestration Dagster pour le pipeline python-duckdb sample
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

# Chemin de base
BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "scripts"


@asset(group_name="sample_pipeline")
def sample_import_data(context: AssetExecutionContext) -> dict:
    """Import des données sources vers DuckDB (sample)"""
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

readme_content = """
# Python DuckDB - Pipeline de données orchestré

Ce projet permet de gérer un flux complet de données depuis l'import de fichiers sources.
Il inclut la transformation et l'agrégation des données pour obtenir des résultats prêts à l'analyse.
Les résultats finaux peuvent être exportés pour usage externe.

## Démarrage rapide

### Exécution classique (manuelle)
```bash
cd macro/
python3 python-run-process.py
```

### Orchestration avec Dagster
```bash
# Lancer l'interface web principale (port 3000)
cd macro/
python3 dagster-run-process-launch.py

# Lancer l'exemple sample (port 3001)
cd sample/macro/
python3 dagster-run-process-sample-launch.py

# Arrêter Dagster
python3 dagster-stop.py
```

**Interfaces web:**
- Principal: http://localhost:3000
- Sample: http://localhost:3001

## Architecture du flux de données

```
    CSV Files               DuckDB Database              CSV Files
    (Source)                  (Processing)                (Output)
       │                          │                          │
       │                          │                          │
       ▼                          ▼                          ▼
  ┌─────────┐              ┌────────────┐            ┌──────────┐
  │ data/   │   source.py  │database.   │  results.py│  data/   │
  │ source/ │─────────────▶│  duckdb    │───────────▶│processed/│
  │         │   (import)   │            │  (export)  │  final/  │
  │ *.csv   │              │  Tables:   │            │  *.csv   │
  └─────────┘              │  - SOURCE  │            └──────────┘
                           │  - TRANS.  │
                           │  - SUMMARY │
                           └────────────┘
                                 │
                                 │ transforming.py
                                 │ (transform & aggregate)
                                 ▼
                           ┌────────────┐
                           │   Python   │
                           │  + Pandas  │
                           │  + DuckDB  │
                           │  (Engine)  │
                           └────────────┘
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
│   └── database.duckdb     Base de données DuckDB principale du projet
│
├── scripts/                
│   ├── source.py           Script d'import des données depuis les fichiers sources
│   ├── transforming.py     Script pour transformer et agréger les données
│   └── results.py          Script pour générer et exporter les résultats finaux
│
├── macro/                  Scripts d'orchestration
│   ├── python-run-process.py              Exécution classique du pipeline
│   ├── dagster-run-process.py             Définition Dagster du pipeline
│   ├── dagster-run-process-launch.py      Lancement de Dagster (port 3000)
│   ├── dagster-stop.py                    Arrêt de tous les processus Dagster
│   ├── dagster.log                        Logs Dagster
│   ├── dagster.pid                        PID du processus Dagster
│   └── execution.log                      Logs d'exécution du pipeline
│
├── sample/                 Dossier contenant un exemple complet avec données de test
│   ├── data/
│   │   └── source/
│   │       └── SAMPLE_DATA.csv           Données d'exemple
│   ├── db/
│   │   └── database.duckdb
│   ├── scripts/            Scripts fonctionnels pour l'exemple
│   └── macro/
│       ├── python-run-process-sample.py
│       ├── dagster-run-process-sample.py
│       └── dagster-run-process-sample-launch.py (port 3001)
│
├── requirements.txt        Dépendances: pandas, duckdb, dagster, dagster-webserver
├── python-duckdb.py        Générateur de structure de projet
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

### dagster-stop.py
Arrête tous les processus Dagster:
- Kill de tous les processus `dagster dev`
- Nettoyage des fichiers PID
- Vérification de l'arrêt complet

## Installation

```bash
pip install -r requirements.txt
```

Dépendances:
- `pandas` - Manipulation de données
- `duckdb` - Base de données analytique
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

## Ports réseau

- **Principal**: Port 3000 (http://localhost:3000)
- **Sample**: Port 3001 (http://localhost:3001)

Les deux instances peuvent fonctionner simultanément sans conflit.

## Logs

- `macro/execution.log` - Logs du pipeline (import, transformation, export)
- `macro/dagster.log` - Logs du serveur Dagster
- `macro/dagster.pid` - PID du processus Dagster actif

"""



# Templates Python pour le projet principal (vides)
python_templates = {
    
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

# Templates Python avec code complet pour le sample
python_templates_sample = {
    
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
    
    "source.py": textwrap.dedent("""
        from pathlib import Path
        import pandas as pd
        import duckdb

        BASE_DIR = Path(__file__).resolve().parent.parent
        DB_PATH = BASE_DIR / "db" / "database.duckdb"
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
                conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file}')")
                
                # Compter les lignes
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                
                print(f"✓ {csv_file.name} importé dans la table {table_name} ({count} lignes)")
            
            conn.close()
            print(f"Import terminé: {len(csv_files)} fichier(s) traité(s)")
        
        if __name__ == "__main__":
            import_data()
        """),
    
    "transforming.py": textwrap.dedent("""
        from pathlib import Path
        import pandas as pd
        import duckdb

        BASE_DIR = Path(__file__).resolve().parent.parent
        DB_PATH = BASE_DIR / "db" / "database.duckdb"

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
                print(f"✓ Table TRANSFORMED_DATA créée ({count} lignes)")
                
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
                print(f"✓ Table SUMMARY_DATA créée ({count} continents)")
                
            except Exception as e:
                print(f"Erreur lors de la transformation: {e}")
            
            conn.close()
        
        if __name__ == "__main__":
            transform_data()
        """),
    
    "results.py": textwrap.dedent("""
        from pathlib import Path
        import pandas as pd
        import duckdb
    
        BASE_DIR = Path(__file__).resolve().parent.parent
        DB_PATH = BASE_DIR / "db" / "database.duckdb"
        PROCESSED_DIR = BASE_DIR / "data" / "processed"
        FINAL_DIR = BASE_DIR / "data" / "final"

        def export_results():
            '''Exporte TRANSFORMED_DATA vers processed/ et SUMMARY_DATA vers final/'''
            
            # Connexion à la base de données
            conn = duckdb.connect(str(DB_PATH))
            
            try:
                # Exporter TRANSFORMED_DATA vers data/processed/
                output_processed = PROCESSED_DIR / "TRANSFORMED_DATA.csv"
                conn.execute(f"COPY TRANSFORMED_DATA TO '{output_processed}' (HEADER, DELIMITER ',')")
                
                count = conn.execute("SELECT COUNT(*) FROM TRANSFORMED_DATA").fetchone()[0]
                print(f"✓ TRANSFORMED_DATA exporté vers processed/ ({count} lignes)")
                
                # Exporter SUMMARY_DATA vers data/final/
                output_final = FINAL_DIR / "SUMMARY_DATA.csv"
                conn.execute(f"COPY SUMMARY_DATA TO '{output_final}' (HEADER, DELIMITER ',')")
                
                count = conn.execute("SELECT COUNT(*) FROM SUMMARY_DATA").fetchone()[0]
                print(f"✓ SUMMARY_DATA exporté vers final/ ({count} lignes)")
                
            except Exception as e:
                print(f"Erreur lors de l'export: {e}")
            
            conn.close()
        
        if __name__ == "__main__":
            export_results()
        """),
}



# Structure pour le sample (différente pour le fichier macro)
structure_sample = {
    "data": ["source", "processed", "final"],
    "db": ["database.duckdb"],
    "scripts": ["source.py", "transforming.py", "results.py"],
    "macro": ["python-run-process-sample.py", "dagster-run-process-sample.py", "dagster-run-process-sample-launch.py", "dagster-stop.py", "execution.log"],
    "README.md": None,
    "requirements.txt": None
}

# Fonction pour créer la structure du projet
def create_structure(base_path, struct, with_sample=False):
    # Choisir le bon template selon si c'est un sample ou non
    templates = python_templates_sample if with_sample else python_templates
    
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
                    # Créer le fichier database.duckdb avec DuckDB
                    if item == "database.duckdb":
                        import duckdb
                        conn = duckdb.connect(str(item_path))
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
                        item_path.write_text(dagster_launch_template_main, encoding='utf-8')
                        # Rendre le fichier exécutable
                        item_path.chmod(0o755)
                    elif item == "dagster-run-process-sample-launch.py":
                        # Template de lancement pour le sample
                        item_path.write_text(dagster_launch_template_sample, encoding='utf-8')
                        # Rendre le fichier exécutable
                        item_path.chmod(0o755)
                    elif item == "dagster-stop.py":
                        # Template pour arrêter Dagster (même pour principal et sample)
                        item_path.write_text(dagster_stop_template, encoding='utf-8')
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
                file_path.write_text(readme_content, encoding='utf-8')
            elif key == "requirements.txt":
                file_path.write_text(requirements_content, encoding='utf-8')
    
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
    base_path = Path.cwd() / PROJECT_NAME
    base_path.mkdir(exist_ok=True)
    
    # Créer la structure principale (sans exemple)
    create_structure(base_path, structure, with_sample=False)
    
    # Créer le sous-dossier sample avec l'exemple
    sample_path = base_path / "sample"
    sample_path.mkdir(exist_ok=True)
    create_structure(sample_path, structure_sample, with_sample=True)
    
    # Copier le script de génération
    current_file = Path(__file__).resolve()
    shutil.copy(current_file, base_path / "python-duckdb.py")
    
    print(f"{PROJECT_NAME} créé avec succès dans {base_path}")
    print(f"✓ Structure principale créée")
    print(f"✓ Exemple disponible dans {PROJECT_NAME}/sample/")
