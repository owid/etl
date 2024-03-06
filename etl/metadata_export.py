import copy
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import pandas as pd
import rich_click as click
from owid.catalog import Dataset, utils
from owid.catalog.yaml_metadata import _merge_variable_metadata
from rich.console import Console
from rich.syntax import Syntax

from etl import paths
from etl.files import ruamel_dump, ruamel_load, yaml_dump


@click.command(name="metadata-export", help=__doc__)
@click.argument(
    "dataset_path",
    type=click.Path(),
)
@click.option(
    "-o",
    "--output",
    type=click.Path(),
    help="Save output into YAML file. If not specified, save to *.meta.yml",
)
@click.option(
    "--show/--no-show",
    default=False,
    type=bool,
    help="Show output instead of saving it into a file.",
)
@click.option(
    "--decimals",
    default="auto",
    type=str,
    help="Add display.numDecimalPlaces to all numeric variables. Use integer or `auto` for autodetection. Disable with `no`.",
)
def cli(
    dataset_path: str,
    output: str,
    show: bool,
    decimals: Optional[str],
) -> None:
    """Export dataset, tables & indicator metadata in YAML format.

    Given a `DATASET_PATH`, load the corresponding dataset and export its metadata in YAML format (including table and indicator metadata). The metadata file and can be later edited manually. If the output YAML already exists, it will be updated with new values.


    **When can this be useful?**
    - This is useful when some metadata fields have been created dynamically in the code and you want to see the final result.
    - To prefill the YAML metadata file with the list of indicators and tables in the dataset. Note that, when first created, an ETL step is not yet aware of the columns of the tables of the dataset. It only knows that once you've executed th step.


    **Example 1:** Save to YAML file `etl/steps/data/garden/ggdc/2020-10-01/ggdc_maddison.meta.yml`

    ```
    etl metadata-export data/garden/ggdc/2020-10-01/ggdc_maddison
    ```

    **Example 2:** Show output instead of saving the file

    ```
    etl metadata-export data/garden/ggdc/2020-10-01/ggdc_maddison --show
    ```
    """
    if show:
        assert not output, "Can't use --show and --output at the same time."
    output = main(dataset_path, output, show, decimals)


def main(
    path: str,
    output: str,
    show: bool,
    decimals: Optional[str],
) -> str:
    """Read docstring of `cli`."""
    ds = Dataset(path=path)
    meta_dict = metadata_export(ds, prune=True, decimals=int(decimals) if decimals.isnumeric() else decimals)  # type: ignore

    output_path = Path(output) if output else paths.STEP_DIR / "data" / f"{ds.metadata.uri}.meta.yml"

    # reorder fields to have consistent YAML
    meta_dict = reorder_fields(meta_dict)

    yaml_str = merge_or_create_yaml(
        meta_dict,
        output_path,
    )

    if show:
        Console().print(Syntax(yaml_str, "yaml", line_numbers=True))
    else:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(yaml_str)  # type: ignore

    return str(output_path)


def merge_or_create_yaml(meta_dict: Dict[str, Any], output_path: Path, delete_empty: bool = True) -> str:
    """Merge metadata with existing YAML file or create a new one and return it as string."""
    # if output_path exists, update its values, but keep YAML structure intact
    if output_path.exists():
        with open(output_path, "r") as f:
            doc = ruamel_load(f) or {}

        if "dataset" not in doc:
            doc["dataset"] = {}
        if "tables" not in doc:
            doc["tables"] = {}

        for k, v in meta_dict.get("dataset", {}).items():
            doc["dataset"][k] = v

        if delete_empty:
            if "update_period_days" in doc["dataset"] and "update_period_days" not in meta_dict.get("dataset", {}):
                del doc["dataset"]["update_period_days"]

        for tab_name, tab_dict in meta_dict.get("tables", {}).items():
            variables = tab_dict.pop("variables", {})

            # update table
            if tab_name not in doc["tables"]:
                doc["tables"][tab_name] = {}
            doc["tables"][tab_name].update(tab_dict)

            # update variables
            if "variables" not in doc["tables"][tab_name]:
                doc["tables"][tab_name]["variables"] = {}
            orig_variables = doc["tables"][tab_name]["variables"]

            for var_name, var_meta in variables.items():
                orig_variables[var_name] = _merge_variable_metadata(
                    orig_variables[var_name],
                    var_meta,
                    if_origins_exist="replace",
                    # we merge display too
                    merge_fields=["presentation", "grapher_config", "display"],
                )

                if delete_empty:
                    if orig_variables[var_name].get("processing_level") == "":
                        del orig_variables[var_name]["processing_level"]

        if doc["dataset"] == {}:
            del doc["dataset"]

        return ruamel_dump(doc)
    else:
        return yaml_dump(meta_dict, replace_confusing_ascii=True)  # type: ignore


def metadata_export(
    ds: Dataset,
    prune: bool = False,
    decimals: Optional[Union[int, Literal["auto", "no"]]] = None,
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

    # default values
    if not ds_meta.get("non_redistributable", False):
        del ds_meta["non_redistributable"]

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

            # add decimals
            if decimals is not None and decimals != "no" and pd.api.types.is_numeric_dtype(tab[col]):
                if "display" not in variable:
                    variable["display"] = {}
                if decimals == "auto":
                    variable["display"]["numDecimalPlaces"] = _guess_decimals(tab[col])
                else:
                    variable["display"]["numDecimalPlaces"] = decimals

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

    meta = {
        "dataset": ds_meta,
        "tables": tb_meta,
    }
    return reorder_fields(meta)


def reorder_fields(m: Dict[str, Any]) -> Dict[str, Any]:
    """Reorder metadata fields to have consistent YAML."""
    # make copy and then modify everything in place
    m = copy.deepcopy(m)

    _reorder_keys(m, ["definitions", "common", "dataset", "tables"])

    origin_order = ["producer", "version_producer", "title", "description", "citation_full"]
    for origin in m.get("definitions", {}).get("common", {}).get("origins", []):
        _reorder_keys(origin, origin_order)

    tab_order = [
        "title",
        "description",
        "variables",
    ]
    var_order = [
        "title",
        "unit",
        "short_unit",
        "display",
        "description_short",
        "description_key",
        "description_from_producer",
        "description_processing",
        "processing_level",
    ]
    presentation_order = [
        "title_public",
        "title_variant",
        "attribution_short",
        "topic_tags",
        "faqs",
        "grapher_config",
    ]
    grapher_config_order = [
        "title",
        "subtitle",
        "variantName",
        "sourceDesc",
        "originUrl",
        "hasMapTab",
        "tab",
        "addCountryMode",
        "yAxis",
        "colorScale",
        "minTime",
        "maxTime",
        "timelineMaxTime",
        "selectedFacetStrategy",
        "hideAnnotationFieldsInTitle",
        "hideRelativeToggle",
        "relatedQuestions",
        "map",
        "selectedEntityNames",
        "selectedEntityColors",
        "note",
        "$schema",
    ]
    for tab in m["tables"].values():
        _reorder_keys(tab, tab_order)
        for var_meta in tab["variables"].values():
            _reorder_keys(var_meta, var_order)
            if "presentation" in var_meta:
                _reorder_keys(var_meta["presentation"], presentation_order)
                _reorder_keys(var_meta["presentation"].get("grapher_config", {}), grapher_config_order)

    return m


def _reorder_keys(d: Dict[str, Any], order: List[str]) -> None:
    keys = order + [k for k in d.keys() if k not in order]
    for k in keys:
        if k in d:
            d[k] = d.pop(k)


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


def _guess_decimals(s: pd.Series, max_decimals=3) -> int:
    """Guess the number of decimals in a series."""
    if pd.api.types.is_integer_dtype(s):
        return 0

    s = s.dropna()
    if s.empty:
        return 0

    assert pd.api.types.is_float_dtype(s)

    for d in range(max_decimals + 1):
        if (s - s.round(d)).abs().max() < 1e-6:
            return d

    return max_decimals


if __name__ == "__main__":
    cli()
