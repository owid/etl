import os

from streamlit.components.v1 import declare_component

_component = declare_component(
    "sentry_loader",
    path=os.path.join(os.path.dirname(__file__), "frontend"),
)


def load_sentry():
    """Mounts the component (and thus injects the scripts)."""
    _component()
