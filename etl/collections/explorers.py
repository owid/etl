from typing import Optional

from etl.config import OWIDEnv
from etl.helpers import PathFinder


def upsert_explorer(
    config: dict, paths: PathFinder, explorer_name: Optional[str] = None, owid_env: Optional[OWIDEnv] = None
) -> None:
    """TODO: Replicate `etl.collections.multidim.upsert_mdim_data_page`."""
    pass
