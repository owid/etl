"""Common tooling for MDIMs/Explorers."""

from sqlalchemy.engine import Engine

from etl.collections.model import Collection
from etl.collections.utils import validate_indicators_in_db


def validate_collection_config(collection: Collection, engine: Engine) -> None:
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
    indicators = collection.indicators_in_use()
    validate_indicators_in_db(indicators, engine)
