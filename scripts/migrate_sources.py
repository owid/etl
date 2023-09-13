import rich_click as click
import structlog
import yaml
from owid.catalog import License, Origin

from etl.snapshot import SnapshotMeta

log = structlog.get_logger()


@click.command(help=__doc__)
@click.argument(
    "path",
    type=click.Path(),
)
def cli(
    path: str,
) -> None:
    """
    Migrate snapshot YAML file from sources to origin.

    Usage:
        python scripts/migrate_sources.py snapshots/ggdc/2020-10-01/ggdc_maddison.xlsx.dvc
    """
    sm = SnapshotMeta.load_from_yaml(path)

    assert not sm.origin, "YAML has been already migrated"

    origin = Origin(
        producer=sm.source_name,
        title=sm.name,
        description=sm.description,
        citation_full=sm.source_published_by,
        url_main=sm.url,
        url_download=sm.source_data_url,
        date_published=sm.publication_date,
        date_accessed=sm.date_accessed,
    )

    # replace empty strings by None
    if getattr(origin, "description_snapshot") == "":
        origin.description_snapshot = None

    del sm.source_name
    del sm.source_published_by
    del sm.name
    del sm.url
    del sm.source_data_url
    del sm.description
    del sm.publication_date
    # we're just dropping this one, shoud be inferred from publication_date
    del sm.publication_year
    del sm.date_accessed

    license = License(
        name=sm.license_name,
        url=sm.license_url,
    )

    del sm.license_name
    del sm.license_url

    sm.origin = origin
    sm.license = license

    if not license.name and not license.url:
        del sm.license

    # is_public is true by default
    if sm.is_public:
        del sm.is_public

    # save YAML file
    sm.save()

    # add original `outs` and `wdir` to the snapshot file
    with open(sm.path, "a") as f:
        yaml.dump(
            {
                "wdir": f"../../../data/snapshots/{sm.namespace}/{sm.version}",
            },
            f,
        )
        yaml.dump(
            {
                "outs": sm.outs,
            },
            f,
        )


if __name__ == "__main__":
    cli()
