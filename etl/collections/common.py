"""Common tooling for MDIMs/Explorers."""

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

import etl.grapher.model as gm
from etl.collections.model import Collection
from etl.collections.utils import validate_indicators_in_db
from etl.db import get_engine


def validate_collection_config(collection: Collection, engine: Engine, tolerate_extra_indicators: bool) -> None:
    """Fundamental validation of the configuration of a collection (explorer or MDIM):

    - Ensure that the views reference valid dimensions.
    - Ensure that there are no duplicate views.
    - Ensure that all indicators in the collection are in the database.

    NOTE: On top of this validation, one may want to apply further validations on MDIMs or Explorers specifically.
    """
    # Ensure that all views are in choices
    collection.validate_views_with_dimensions()

    # Validate duplicate views
    collection.check_duplicate_views()

    # Check that all indicators in mdim exist
    indicators = collection.indicators_in_use(tolerate_extra_indicators)
    validate_indicators_in_db(indicators, engine)


def map_indicator_path_to_id(catalog_path: str) -> str | int:
    # Check if given path is actually an ID
    if str(catalog_path).isdigit():
        return catalog_path

    # Get ID, assuming given path is a catalog path
    engine = get_engine()
    with Session(engine) as session:
        db_indicator = gm.Variable.from_id_or_path(session, catalog_path)
        assert db_indicator.id is not None
        return db_indicator.id
