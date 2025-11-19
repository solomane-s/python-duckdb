
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
        f.write("\n")

    logger.info("Début du process")

    import_data()
    transform_data()
    export_results()

    logger.info("Fin du process")
    duration = datetime.now() - start_time
    logger.info(f"Durée du process : {duration}")

    with open(LOG_PATH, "a") as f:
        f.write("\n")

    print(f"Process terminé {duration}")

if __name__ == "__main__":
    run_process()
