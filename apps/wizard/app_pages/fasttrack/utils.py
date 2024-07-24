"""Utils."""
import os
from typing import Any, Dict, Optional

import streamlit as st
from cryptography.fernet import Fernet
from structlog import get_logger

# Logger
log = get_logger()
# Read file methods
IMPORT_GSHEET = "import_gsheet"
UPDATE_GSHEET = "update_gsheet"
LOCAL_CSV = "local_csv"


def _get_secret_key() -> Optional[Fernet]:
    secret_key = os.environ.get("FASTTRACK_SECRET_KEY")
    if not secret_key:
        log.warning("FASTTRACK_SECRET_KEY not found in environment variables. Not using encryption.")
        return None
    return Fernet(secret_key)


FERNET_KEY = _get_secret_key()


def _encrypt(s: str) -> str:
    fernet = FERNET_KEY
    return fernet.encrypt(s.encode()).decode() if fernet else s


def _decrypt(s: str) -> str:
    fernet = FERNET_KEY
    # content is not encrypted, this is to keep it backward compatible with old datasets
    # that weren't using encryption
    if "docs.google.com" in s:
        return s
    else:
        return fernet.decrypt(s.encode()).decode() if fernet else s


def set_states(states_values: Dict[str, Any]):
    for key, value in states_values.items():
        st.session_state[key] = value


class ValidationError(Exception):
    pass
