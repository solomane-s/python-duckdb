"""
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
