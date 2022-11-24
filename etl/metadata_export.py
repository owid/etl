import os
from typing import Any, Dict, cast

import click
import yaml
from owid.catalog import Dataset


@click.command(help=__doc__)
@click.argument(
    "path",
    type=click.Path(),
)
@click.option(
    "-o",
    "--output",
    type=click.Path(),
    help="Save output into YAML file",
)
def cli(
    path: str,
    output: str,
) -> None:
    """Export dataset & tables & columns metadata in YAML format. This
    is useful for generating *.meta.yml files that can be later manually edited.

    Usage:
        etl-metadata-export data/garden/ggdc/2020-10-01/ggdc_maddison -o etl/steps/data/garden/ggdc/2020-10-01/ggdc_maddison.meta.yml
    """
    meta_str = metadata_export(str(path))
    if output:
        os.makedirs(os.path.dirname(output), exist_ok=True)
        with open(output, "w") as f:
            f.write(meta_str)
    else:
        print(meta_str)


def metadata_export(
    path: str,
) -> str:
    ds = Dataset(path)

    ds_meta = ds.metadata.to_dict()

    # transform dataset metadata
    for source in ds_meta["sources"]:
        _prune_empty(source)

    for license in ds_meta.get("licenses", []):
        _prune_empty(license)

    ds_meta.pop("is_public")
    ds_meta.pop("source_checksum")
    # move sources at the end
    ds_meta["sources"] = ds_meta.pop("sources")

    # transform tables metadata
    tb_meta = {}
    for tab in ds:
        t = tab.metadata.to_dict()
        t.pop("short_name")
        t.pop("dataset")
        t.pop("primary_key", None)

        # transform variables metadata
        t["variables"] = {}
        for col in tab.columns:
            if col in ("country", "year"):
                continue
            variable = tab[col].metadata.to_dict()

            # add required units
            variable.setdefault("short_unit", "")
            variable.setdefault("unit", "")

            for source in variable.get("sources", []):
                _prune_empty(source)

            # move sources at the end
            if "sources" in variable:
                variable["sources"] = variable.pop("sources")

            t["variables"][col] = variable

        tb_meta[tab.metadata.short_name] = t

    final = {
        "dataset": ds_meta,
        "tables": tb_meta,
    }

    return cast(str, yaml.dump(final, sort_keys=False, allow_unicode=True))


def _prune_empty(d: Dict[str, Any]) -> None:
    """Remove empty values from dict."""
    for k, v in list(d.items()):
        if not v:
            del d[k]


if __name__ == "__main__":
    cli()
