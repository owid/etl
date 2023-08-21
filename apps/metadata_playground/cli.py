"""Interface to run the app from python.

This module is implemented so that we can run the app with the `python` keyword:

python cli.py
"""
import sys

from streamlit.web import cli as stcli

from etl.paths import APPS_DIR
from etl.config import METAPLAY_PORT

SCRIPT_PATH = APPS_DIR / "metadata_playground" / "app.py"

print(SCRIPT_PATH)
def cli():
    """Run app."""
    sys.argv = ["streamlit", "run", str(SCRIPT_PATH), "--server.port", str(METAPLAY_PORT)]
    sys.exit(stcli.main())
