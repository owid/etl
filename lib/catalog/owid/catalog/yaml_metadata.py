from pathlib import Path
from typing import Any, List, Literal, Union

from owid.catalog.meta import SOURCE_EXISTS_OPTIONS

from .meta import DatasetMeta, TableMeta, VariableMeta
from .tables import Table
from .utils import dynamic_yaml_load, dynamic_yaml_to_dict


def update_metadata_from_yaml(
    tb: Table,
    path: Union[Path, str],
    table_name: str,
    extra_variables: Literal["raise", "ignore"] = "raise",
    if_origins_exist: SOURCE_EXISTS_OPTIONS = "replace",
) -> None:
    """Update metadata of table and variables from a YAML file.

    The logic is as follows:

    - Whatever you write in the definitions.common section of the yaml file always replaces
      the metadata that was on the table already.
    - Whatever you write in the tables section of the yaml file always replaces what was
      written in the definitions.common section of the yaml file.

    See more details [here](https://github.com/owid/etl/pull/1737#issuecomment-1750245399)

    :param path: Path to YAML file.
    :param table_name: Name of table, also updates this in the metadata.
    """
    # load YAML file as dictionary, add parameters from dataset metadata
    # TODO: tb.metadata.dataset reference shouldn't exist
    annot = dynamic_yaml_to_dict(
        dynamic_yaml_load(path, DatasetMeta._params_yaml(tb.metadata.dataset or DatasetMeta()))
    )

    tb.metadata.short_name = table_name

    t_annot = annot["tables"][table_name]

    # validation
    if extra_variables == "raise":
        _validate_variables(t_annot, tb)

    common_dict = annot.get("definitions", {}).get("common", {})

    # update variables
    for v_short_name in tb.columns:
        meta_dict = tb[v_short_name].m.to_dict()

        # first overwrite table metadata with definitions.common
        meta_dict = _merge_variable_metadata(meta_dict, common_dict, if_origins_exist=if_origins_exist, overwrite=True)

        # then overwrite with table specific metadata
        variable_dict = t_annot.get("variables", {}).get(v_short_name, {})
        meta_dict = _merge_variable_metadata(
            meta_dict, variable_dict, if_origins_exist=if_origins_exist, overwrite=True
        )

        # we allow `- *descriptions` which needs to be flattened
        if "description_key" in meta_dict:
            meta_dict["description_key"] = _flatten(meta_dict["description_key"])

        # convert to objects
        tb[v_short_name].metadata = VariableMeta.from_dict(meta_dict)

    # update table attributes
    tb_meta_dict = _merge_table_metadata(tb.m.to_dict(), t_annot)
    tb.metadata = TableMeta.from_dict(tb_meta_dict)


def _merge_variable_metadata(md: dict, new: dict, if_origins_exist: SOURCE_EXISTS_OPTIONS, overwrite: bool) -> dict:
    """Merge VariableMeta in a dictionary with another dictionary. It modifies the original object."""
    # NOTE: when this gets stable, consider removing flags `if_origins_exist` and `overwrite` if they are not used
    for k, v in new.items():
        # special cases
        if k in ("presentation", "grapher_config"):
            # merge fields
            md[k] = _merge_variable_metadata(md.get(k, {}), v, if_origins_exist, overwrite)
        # origins have their special flag to decide what to do
        elif k == "origins":
            if if_origins_exist == "fail" and md["origins"]:
                raise ValueError(f"Origins already exist for variable {k}")
            elif if_origins_exist == "replace":
                md["origins"] = v
            else:
                md["origins"] = md.get("origins", []) + v
        # explicitly passing empty list means we want to overwrite
        elif v == []:
            md[k] = []
        # append or overwrite list (could be origins, sources or others)
        elif isinstance(v, list):
            if overwrite:
                md[k] = v
            else:
                md[k] = md.get(k, []) + v
        # key is already defined in metadata
        elif k in md:
            # should we overwrite it?
            if overwrite:
                md[k] = v
        # otherwise just define it
        else:
            md[k] = v
    return md


def _merge_table_metadata(meta: dict, new: dict) -> dict:
    """Merge TableMeta in a dictionary with another dictionary. It modifies the original object."""
    for k, v in new.items():
        if k != "variables":
            meta[k] = v
    return meta


def _validate_variables(t_annot: dict, tb: Table) -> None:
    yaml_variable_names = t_annot.get("variables", {}).keys()
    table_variable_names = tb.columns
    extra_variable_names = yaml_variable_names - table_variable_names
    if extra_variable_names:
        raise ValueError(f"Table {tb.metadata.short_name} has extra variables: {extra_variable_names}")


def _flatten(lst: List[Any]) -> List[str]:
    """Flatten list that contains either strings or lists."""
    return [item for sublist in lst for item in ([sublist] if isinstance(sublist, str) else sublist)]
