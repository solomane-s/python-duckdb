
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
        conn.execute("""
            CREATE OR REPLACE TABLE TRANSFORMED_DATA AS
            SELECT 
                *,
                ROUND(population / 1000000.0, 2) AS population_millions,
                salaire_moyen * 12 AS salaire_annuel
            FROM SAMPLE_DATA
        """)

        count = conn.execute("SELECT COUNT(*) FROM TRANSFORMED_DATA").fetchone()[0]
        print(f"✓ Table TRANSFORMED_DATA créée ({count} lignes)")

        # Transformation 2: Créer SUMMARY_DATA (agrégation par continent)
        conn.execute("""
            CREATE OR REPLACE TABLE SUMMARY_DATA AS
            SELECT 
                continent,
                ROUND(SUM(population) / 1000000.0, 2) AS population_totale,
                CAST(ROUND(AVG(salaire_moyen), 0) AS INTEGER) AS salaire_moyen,
                COUNT(*) AS nombre_pays
            FROM SAMPLE_DATA
            GROUP BY continent
            ORDER BY continent
        """)

        count = conn.execute("SELECT COUNT(*) FROM SUMMARY_DATA").fetchone()[0]
        print(f"✓ Table SUMMARY_DATA créée ({count} continents)")

    except Exception as e:
        print(f"Erreur lors de la transformation: {e}")

    conn.close()

if __name__ == "__main__":
    transform_data()
