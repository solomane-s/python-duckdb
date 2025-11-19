
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
