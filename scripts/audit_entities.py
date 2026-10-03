import click
import pandas as pd
import structlog
from owid.catalog import Dataset

from etl.db import get_engine
from etl.paths import DATA_DIR

log = structlog.get_logger()


@click.command()
def audit_entities_cli() -> None:
    """Compare countries_regions.csv against database and find any mismatch in entity ids."""
    # region -> entity from countries regions
    csv_df = Dataset(DATA_DIR / "garden/regions/2023-01-01/regions")["regions"].rename(
        columns={"legacy_entity_id": "entity_id"}
    )

    assert csv_df.name.value_counts().max() == 1
    csv_map = csv_df.set_index("name").entity_id.dropna().astype(int).to_dict()

    # region -> entity from DB
    engine = get_engine()
    q = """
    select
        id as entity_id,
        name
    from entities
    """
    db_df = pd.read_sql(q, engine)
    assert db_df.name.value_counts().max() == 1
    db_map = db_df.set_index("name").entity_id.to_dict()

    for name, entity_id in csv_map.items():
        if entity_id != db_map[name]:
            log.warning(
                "entity_id mismatch",
                name=name,
                csv_entity_id=entity_id,
                db_entity_id=db_map[name],
            )


if __name__ == "__main__":
    audit_entities_cli()
