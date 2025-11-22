
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
