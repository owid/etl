import copy
import os
from typing import Any, Dict, Tuple

import pandas as pd
import rich_click as click
from owid.catalog import Dataset, utils

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
    meta_str = metadata_export(ds, prune=True)
    if output:
        os.makedirs(os.path.dirname(output), exist_ok=True)
        with open(output, "w") as f:
            f.write(yaml_dump(meta_str, replace_confusing_ascii=True))  # type: ignore
    else:
        print(meta_str)


def metadata_export(
    ds: Dataset,
    prune: bool = False,
) -> dict:
    """
    :param prune: If True, remove origins and licenses that would be propagated from the snapshot.
    """
    ds_meta = ds.metadata.to_dict()

    # transform dataset metadata
    for source in ds_meta.get("sources", []):
        _prune_empty(source)

    for origin in ds_meta.get("origins", []):
        _prune_empty(origin)

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
    if "sources" in ds_meta:
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
        used_titles = {tab[col].metadata.title for col in tab.columns if tab[col].metadata.title}
        for col in tab.columns:
            if col in ("country", "year"):
                continue
            variable = tab[col].metadata.to_dict()

            if "display" in variable:
                display = variable["display"]
                # move units and short units from display
                if not variable.get("unit"):
                    variable["unit"] = display.pop("unit", "")
                if not variable.get("short_unit"):
                    variable["short_unit"] = display.pop("shortUnit", "")

                # includeInTable: true is default, no need to have it here
                if display.get("includeInTable"):
                    display.pop("includeInTable")

                # if title is underscored and identical to column name, try to use display name as title
                if (
                    col == variable["title"]
                    and utils.underscore(variable["title"]) == variable["title"]
                    and display.get("name")
                    and display["name"] not in used_titles
                ):
                    variable["title"] = display.pop("name")

                if not display:
                    variable.pop("display")

            # we can't have duplicate titles
            if "title" in variable:
                used_titles.add(variable["title"])

            # remove empty descriptions and short units
            if variable.get("description") == "":
                variable.pop("description", None)
            if variable.get("short_unit") == "":
                variable.pop("short_unit", None)

            variable.pop("additional_info", None)

            # add required units
            variable.setdefault("unit", "")

            for source in variable.get("sources", []):
                _prune_empty(source)

            # move sources at the end
            if "sources" in variable:
                variable["sources"] = variable.pop("sources")

            # fix sources
            for source in variable.get("sources", []):
                if "date_accessed" in source:
                    source["date_accessed"] = pd.to_datetime(source["date_accessed"], dayfirst=True).date()

            t["variables"][col] = variable

        tb_meta[tab.metadata.short_name] = t

    ds_meta, tb_meta = _move_sources_to_dataset(ds_meta, tb_meta)

    # remove metadata that is propagated from the snapshot
    # TODO: pruning would be ideally True by default, but we still need some backward compatibility
    if prune:
        ds_meta.pop("description", None)
        ds_meta.pop("origins", None)
        ds_meta.pop("licenses", None)

        for tab in ds:
            assert tab.metadata.short_name
            for var_meta in tb_meta[tab.metadata.short_name]["variables"].values():
                var_meta.pop("origins", None)
                var_meta.pop("license", None)

    return {
        "dataset": ds_meta,
        "tables": tb_meta,
    }


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
    if not all([sources is not None and len(sources) == 1 for sources in vars_sources]):
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
