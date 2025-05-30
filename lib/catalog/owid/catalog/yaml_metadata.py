import io
from pathlib import Path
from typing import Any, Literal

import yaml

from owid.catalog.meta import SOURCE_EXISTS_OPTIONS

from .meta import DatasetMeta, TableMeta, VariableMeta
from .tables import Table
from .utils import dynamic_yaml_load, dynamic_yaml_to_dict


def update_metadata_from_yaml(
    tb: Table,
    path: Path | str,
    table_name: str,
    yaml_params: dict[str, Any] | None = None,
    extra_variables: Literal["raise", "ignore"] = "raise",
    if_origins_exist: SOURCE_EXISTS_OPTIONS = "replace",
) -> None:
    """Update metadata of table and variables from a YAML file.

    Metadata in `common` overwrites each other as follows:

    - Metadata from `definitions.common` always overwrites
      the metadata that was on the table already.
    - Metadata from `tables.<table_name>.common` always overwrites the metadata
      on the table and the metadata from `definitions.common`.
    - Metadata from `tables.<table_name>.variables.<variable_name>` always overwrites
      any metadata that was on the variable already.

    :param path: Path to YAML file.
    :param table_name: Name of table, also updates this in the metadata.
    """
    # Add parameters from dataset metadata
    params = DatasetMeta._params_yaml(tb.metadata.dataset or DatasetMeta())
    params.update(yaml_params or {})

    # Add definitions from shared.meta.yml if it exists
    path_or_io = merge_with_shared_meta(Path(path))

    # load YAML file as dictionary
    # TODO: tb.metadata.dataset reference shouldn't exist
    annot = dynamic_yaml_to_dict(dynamic_yaml_load(path_or_io, params))

    tb.metadata.short_name = table_name

    t_annot = annot.get("tables", {}).get(table_name, {})

    # validation
    if extra_variables == "raise":
        _validate_variables(t_annot, tb)

    common_dict = annot.get("definitions", {}).get("common", {})
    table_common_dict = t_annot.get("common", {})

    # update variables
    for v_short_name in tb.columns:
        meta_dict = tb[v_short_name].m.to_dict()

        # first overwrite table metadata with definitions.common
        meta_dict = _merge_variable_metadata(meta_dict, common_dict, if_origins_exist=if_origins_exist)

        # then overwrite with table specific common
        meta_dict = _merge_variable_metadata(meta_dict, table_common_dict, if_origins_exist=if_origins_exist)

        # then overwrite with table specific metadata
        variable_dict = (t_annot.get("variables") or {}).get(v_short_name, {})
        meta_dict = _merge_variable_metadata(meta_dict, variable_dict, if_origins_exist=if_origins_exist)

        # we allow `- *descriptions` which needs to be flattened
        if "description_key" in meta_dict:
            meta_dict["description_key"] = _flatten(meta_dict["description_key"])

            # make sure all elements are strings
            # NOTE: this could be further flattened if we run into it
            assert all(isinstance(x, str) for x in meta_dict["description_key"])

        # convert to objects
        tb[v_short_name].metadata = VariableMeta.from_dict(meta_dict)

    # update table attributes
    tb_meta_dict = _merge_table_metadata(tb.m.to_dict(), t_annot)
    tb.metadata = TableMeta.from_dict(tb_meta_dict)


def merge_with_shared_meta(path: Path) -> io.StringIO | Path:
    """Merge metadata with shared.meta.yml if it exists."""
    shared_meta_path = path.parent / "shared.meta.yml"
    if shared_meta_path.exists():
        # Load shared.meta.yml
        with open(shared_meta_path) as f:
            shared = yaml.safe_load(f)

        # Load the original meta file
        with open(path) as f:
            meta = yaml.safe_load(f)

        if "definitions" not in meta and "definitions" in shared:
            meta["definitions"] = {}

        if "macros" not in meta and "macros" in shared:
            meta["macros"] = ""

        # Combine definitions
        for k, v in shared.items():
            # Append macros
            if k == "macros":
                meta["macros"] = v + meta["macros"]

            # Combine definitions
            elif k == "definitions":
                for kk, vv in v.items():
                    meta["definitions"].setdefault(kk, vv)

            # Other fields are not supported yet
            else:
                raise NotImplementedError(f"Cannot merge {k} from shared.meta.yml")

        # Create a new file with combined definitions
        s = io.StringIO()
        yaml.dump(meta, s, default_flow_style=False)

        s.seek(0)

        return s
    else:
        return path


def _merge_variable_metadata(
    md: dict,
    new: dict,
    if_origins_exist: SOURCE_EXISTS_OPTIONS,
    merge_fields: list[str] = ["presentation", "grapher_config"],
) -> dict:
    """Merge VariableMeta in a dictionary with another dictionary. It modifies the original object."""
    # NOTE: when this gets stable, consider removing flag `if_origins_exist`
    for k, v in new.items():
        # fields that will be merged
        if k in merge_fields:
            md[k] = _merge_variable_metadata(md.get(k, {}), v, if_origins_exist)
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
            md[k] = v
        # key is already defined in metadata
        elif k in md:
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
    yaml_variable_names = set((t_annot.get("variables") or {}).keys())
    table_variable_names = set(tb.columns)
    extra_yaml_variable_names = yaml_variable_names - table_variable_names
    extra_table_variable_names = table_variable_names - yaml_variable_names
    if extra_yaml_variable_names:
        msg = (
            f"Table {tb.metadata.short_name} has extra variables in YAML file {sorted(list(extra_yaml_variable_names))}"
        )
        if extra_table_variable_names:
            msg += f" and extra variables in table {sorted(list(extra_table_variable_names))}"
        raise ValueError(msg)


def _flatten(lst: list[Any]) -> list[str]:
    """Flatten list that contains either strings or lists."""
    return [item for sublist in lst for item in ([sublist] if isinstance(sublist, str) else sublist)]
