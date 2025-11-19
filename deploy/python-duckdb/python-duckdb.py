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
    "macro": ["process.py", "execution.log"],
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


# README.md
requirements_content = """pandas
duckdb
"""

readme_content = """

- Ce projet permet de gérer un flux complet de données depuis l'import de fichiers sources.
- Il inclut la transformation et l'agrégation des données pour obtenir des résultats prêts à l'analyse.
- Les résultats finaux peuvent être exportés pour usage externe.

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

Moteur de traitement: Python + Pandas + DuckDB
```


## Structure du dossier

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
├── macro/                  
│   ├── process.py          Script principal qui lance import, transformation et export
│   └── execution.log       Fichier journal pour enregistrer les logs d'exécution
│
├── sample/                 Dossier contenant un exemple complet de données et scripts
│
├── requirements.txt        Fichier listant les dépendances du projet
├── python-duckdb.py        Script pour créer toute la structure du projet
└── README.md               Description des dossiers et fichiers du projet
```

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
    
    "process.py": textwrap.dedent("""
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
    
    "process.py": textwrap.dedent("""
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
    create_structure(sample_path, structure, with_sample=True)
    
    # Copier le script de génération
    current_file = Path(__file__).resolve()
    shutil.copy(current_file, base_path / "python-duckdb.py")
    
    print(f"{PROJECT_NAME} créé avec succès dans {base_path}")
    print(f"✓ Structure principale créée")
    print(f"✓ Exemple disponible dans {PROJECT_NAME}/sample/")
