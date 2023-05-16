from datetime import datetime
from typing import Any, Dict, List

from sqlmodel import Session

import etl.grapher_model as gm
from etl.config import GRAPHER_USER_ID
from etl.db import get_engine
