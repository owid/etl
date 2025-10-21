"""Define common, non-secret environment defaults.

This file runs automatically before other modules (like etl.config),
setting shared defaults that can be overridden by user or .env values.
"""
import os

# Default plotly renderer that works on VSCode's interactive window and jupyter notebooks, and lets MkDocs display interactive charts.
# Other renderers (e.g. "vscode", or "plotly_mimetype") may work on VSCode, but then the output doesn't show up on MkDocs.
# NOTE: To override the rendered:
# import plotly.io as pio
# pio.renderers.default = "..."
os.environ.setdefault("PLOTLY_RENDERER", "notebook_connected")
