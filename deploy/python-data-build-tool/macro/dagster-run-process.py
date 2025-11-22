"""
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
