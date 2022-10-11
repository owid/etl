from pathlib import Path

CURRENT_DIR = Path(__file__).parent
# Version of current garden datasets to be created.
VERSION = str(CURRENT_DIR.name)
# Version of meadow datasets to be imported.
MEADOW_VERSION = VERSION
