import copy
import os
from typing import Any, Dict, Tuple

import rich_click as click
from owid.catalog import Dataset

from etl.files import yaml_dump


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
    ds = Dataset(path)
    meta_str = metadata_export(ds)
    if output:
        os.makedirs(os.path.dirname(output), exist_ok=True)
        with open(output, "w") as f:
            f.write(meta_str)
    else:
        print(meta_str)


def metadata_export(
    ds: Dataset,
) -> str:
    ds_meta = ds.metadata.to_dict()

    # transform dataset metadata
    for source in ds_meta.get("sources", []):
        _prune_empty(source)

    for license in ds_meta.get("licenses", []):
        _prune_empty(license)

    # don't export metadata that is inferred from path
    ds_meta.pop("namespace")
    ds_meta.pop("short_name")
    ds_meta.pop("version")
    ds_meta.pop("channel", None)
    ds_meta.pop("additional_info", None)

    ds_meta.pop("is_public")
    ds_meta.pop("source_checksum", None)
    # move sources at the end
    ds_meta["sources"] = ds_meta.pop("sources", [])

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

            variable.pop("additional_info", None)

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

    ds_meta, tb_meta = _move_sources_to_dataset(ds_meta, tb_meta)

    final = {
        "dataset": ds_meta,
        "tables": tb_meta,
    }

    return yaml_dump(final)  # type: ignore


def _move_sources_to_dataset(ds_meta: Dict[str, Any], tb_meta: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """If all variables have the same source, move it to dataset. Otherwise return
    the original metadata.
    """
    ds_meta_orig = copy.deepcopy(ds_meta)
    tb_meta_orig = copy.deepcopy(tb_meta)

    # only works for a single table
    if len(tb_meta) != 1:
        return ds_meta_orig, tb_meta_orig

    tb = list(tb_meta.values())[0]
    vars_sources = [var_meta.pop("sources", None) for var_meta in tb["variables"].values()]

    # every one must have a single source
    if not all([len(sources) == 1 for sources in vars_sources]):
        return ds_meta_orig, tb_meta_orig

    vars_sources = [sources[0] for sources in vars_sources]

    # all must have the same title
    if not all([sources["name"] == vars_sources[0]["name"] for sources in vars_sources]):
        return ds_meta_orig, tb_meta_orig

    ds_meta["sources"] = [vars_sources[0]]

    return ds_meta, tb_meta


def _prune_empty(d: Dict[str, Any]) -> None:
    """Remove empty values from dict."""
    for k, v in list(d.items()):
        if not v:
            del d[k]


if __name__ == "__main__":
    cli()
