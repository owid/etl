from pathlib import Path

from structlog import get_logger

log = get_logger()

NAMESPACE = "cait"
CURRENT_DIR = Path(__file__).parent
VERSION = CURRENT_DIR.name
