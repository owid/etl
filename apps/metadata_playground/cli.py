"""Interface"""
import os
import sys

from streamlit.web import cli as stcli

from etl.paths import APPS_DIR

PORT = 8051
SCRIPT_PATH = APPS_DIR / "metadata_playground" / "app.py"

print(SCRIPT_PATH)
def cli():
    """Run app."""
    sys.argv = ["streamlit", "run", str(SCRIPT_PATH), "--server.port", str(PORT)]
    sys.exit(stcli.main())
