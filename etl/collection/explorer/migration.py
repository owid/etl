"""Migrate old explorer config to new one."""

import re
from collections import defaultdict
from copy import deepcopy
from typing import Any, Dict, List, Tuple

import pandas as pd
from owid.catalog.utils import underscore
from sqlalchemy.orm import Session
from structlog import get_logger

from etl.config import OWID_ENV, OWIDEnv
from etl.grapher import model as gm

log = get_logger()


PATTERN_CSV = r"https://catalog\.ourworldindata\.org/(?P<group>[^/]+/[^/]+/[^/]+/[^/]+/[^/]+)\.csv"


class ExplorerMigration:
    def __init__(self, explorer_as_json: Dict[str, Any], slug: str):
        """Parse DB"""
        self.slug = slug
        self.explorer_as_json = explorer_as_json
        self.config = self.extract_config()
        self.grapher_block, self.table_columns_block = self.get_blocks()
        self.types = self.get_explorer_types()
        self._sanity_checks()

    def extract_config(self):
        config = {}
        for key, value in self.explorer_as_json.items():
            if (key not in {"_version", "blocks"}) and not key.startswith("#"):
                config[key] = value
        return config

    def get_blocks(self) -> Tuple[Any, List[Any]]:
        """Get grapher and table/columns blocks."""
        # Get blocks
        blocks = self.explorer_as_json.get("blocks", [])
        if not blocks:
            raise ValueError(f"{self.slug}: No blocks found")

        # Get grapher and tables_columns block
        grapher_block = None
        table_columns_block = []
        for block in blocks:
            if block["type"] == "graphers":
                if grapher_block is None:
                    grapher_block = block
                else:
                    raise ValueError("Multiple graphers block found")
            elif block["type"] in {"table", "columns"}:
                table_columns_block.append(block)
            else:
                raise ValueError(f"{self.slug}: Unknown block type: {block['type']}")

        return grapher_block, table_columns_block

    def get_explorer_types(self):
        """Get types of the explorer.

        It is typically either CSV, Indicator or Grapher. But occasionally it can be a hybrid of these. Example: Grapher and CSV.
        """
        # Inspect record to detect explorer type
        assert self.grapher_block is not None, "No grapher block found"
        cols = pd.DataFrame(self.grapher_block["block"]).columns

        explorer_types = []
        if "grapherId" in cols:
            explorer_types.append("grapher")
        if ("yVariableIds" in cols) or ("xVariableId" in cols):
            explorer_types.append("indicator")
        elif len(self.table_columns_block) > 0:
            explorer_types.append("csv")

        return set(explorer_types)

    def _sanity_checks(self):
        """Sanity check that the explorer configuration makes sense."""
        # Grapher block
        if self.grapher_block is None:
            raise ValueError(f"{self.slug}: No grapher block found")

        # Check required columns
        if self.types == {"csv"}:
            columns = pd.DataFrame(self.grapher_block["block"]).columns
            if "tableSlug" not in columns:
                raise ValueError(f"{self.slug}: tableSlug not found in grapher block")
            if "tableSlugs" in columns:
                raise ValueError(f"{self.slug}: tableSlugs not supported at the moment. Please report!")

        # Table/columns block
        ## 1) If no table/column is given, it must be indicator- or grapher-based
        if len(self.table_columns_block) == 0:
            if self.types != {"indicator"} and self.types != {"grapher"}:
                raise ValueError(f"{self.slug}: You must provide a table or columns block for CSV-based explorers")
        else:
            ## 2) table/columns is not accepted for grapher-based
            if self.types == {"grapher"}:
                raise ValueError(f"{self.slug}: table/column block not expected for grapher-based explorers")
            # ## 3) length of table/columns must be an even number, at least 2, except for indicator-based
            # elif self.types != {"indicator"}:
            #     # 3.1) table/columns must be of length 2 at least
            #     if len(self.table_columns_block) < 2:
            #         raise ValueError(f"{self.slug}: Table/columns block must be of length 2 at least")
            #     # 3.2) table/columns must be of length 2n
            #     if len(self.table_columns_block) % 2 != 0:
            #         raise ValueError(f"{self.slug}: Table/columns block must be of length 2n")

    def run_csv(self):
        """Build dictionary like:

        {"uri": display settings}
        """
        # Iterate over all blocks
        display_settings = {}
        table_slugs = {}

        # Get table information
        for block in self.table_columns_block:
            if block["type"] == "table":
                # Sanity checks
                assert len(block["args"]) == 2

                # Relevant information about table
                table_uri = _extract_table_uri(block["args"][0])
                table_slug = block["args"][1]

                # Save table slug to URI mapping
                table_slugs[table_slug] = table_uri

        # Get columns information
        for block in self.table_columns_block:
            if block["type"] == "columns":
                # Sanity checks
                assert (
                    len(block["args"]) == 1
                ), f"columns is expected to have only one argument. Instead got: {block['args']}"

                # Get table URI
                table_slug = block["args"][0]
                table_uri = table_slugs[table_slug]

                for indicator in block["block"]:
                    indicator_uri = f"{table_uri}#{indicator['slug']}"
                    _display_settings = {k: v for k, v in indicator.items() if k != "slug"}
                    display_settings[indicator_uri] = _display_settings

        # Get graphers information
        dimension_slug_to_raw_name = {}
        dimensions = []
        df = pd.DataFrame(self.grapher_block["block"])
        for column in df.columns:
            if column.endswith(" Dropdown"):
                ui_type = "dropdown"
            elif column.endswith(" Radio"):
                ui_type = "radio"
            elif column.endswith(" Checkbox"):
                ui_type = "checkbox"
            else:
                continue

            # Get name, then underscore it for slug
            dim_name = " ".join(column.split()[:-1])
            dim_slug = underscore(str(dim_name))
            # Get choices
            choices = []
            choices_raw = df[column].unique()
            if ui_type == "checkbox":
                assert len(choices_raw) == 2, f"{self.slug}: Checkbox must have 2 choices"
            for choice_name in choices_raw:
                # Sanity check when is checkbox
                if ui_type == "checkbox":
                    assert choice_name in {
                        "true",
                        "false",
                    }, f"{self.slug}: Checkbox must have 'true' and 'false' choices"

                # Missing dimension value transaltes into empty string in TSV
                if pd.isna(choice_name):
                    choice_name = ""
                    choice_slug = ""
                else:
                    choice_slug = underscore(str(choice_name))

                # Define choice
                choices.append(
                    {
                        "slug": choice_slug,
                        "name": choice_name,
                    }
                )

            # Prepare presentation (for dimension)
            presentation = {
                "type": ui_type,
            }
            if ui_type == "checkbox":
                presentation["choice_slug_true"] = "true"

            # Build dimension element
            dimensions.append(
                {
                    "slug": dim_slug,
                    "name": dim_name,
                    "choices": choices,
                    "presentation": presentation,
                }
            )

            # For reference
            dimension_slug_to_raw_name[dim_slug] = column

        # Get views
        views = []

        def _bake_indicator(table_slug, indicator_slug):
            indicator_uri = f"{table_slugs[table_slug]}#{indicator_slug}"
            indicator = {
                "catalogPath": indicator_uri,
            }
            if indicator_uri in display_settings:
                # Use deepcopy so each indicator gets an independent display copy.
                indicator["display"] = deepcopy(display_settings[indicator_uri])
            return indicator

        for block in self.grapher_block["block"]:
            # Get indicators
            table_slug = block["tableSlug"]
            indicators = defaultdict(list)
            if "ySlugs" in block:
                y_slugs = block["ySlugs"].split()
                for y in y_slugs:
                    indicator = _bake_indicator(table_slug, y)
                    indicators["y"].append(indicator)
            if "xSlug" in block:
                slugs = block["xSlug"].split()
                assert len(slugs) == 1
                indicator = _bake_indicator(table_slug, slugs[0])
                indicators["x"].append(indicator)
            if "colorSlug" in block:
                slugs = block["colorSlug"].split()
                assert len(slugs) == 1
                indicator = _bake_indicator(table_slug, slugs[0])
                indicators["color"].append(indicator)
            if "sizeSlug" in block:
                slugs = block["sizeSlug"].split()
                assert len(slugs) == 1
                indicator = _bake_indicator(table_slug, slugs[0])
                indicators["size"].append(indicator)

            indicators = dict(indicators)

            # Get dimensions
            dimensions_view = {}
            for dim in dimensions:
                raw_name = dimension_slug_to_raw_name[dim["slug"]]
                # NOTE: Missing dimension value should translate into empty string, right?
                if raw_name not in block:
                    dimensions_view[dim["slug"]] = ""
                else:
                    choice_slug = underscore(block[raw_name])
                    if dim["presentation"]["type"] == "checkbox":
                        assert choice_slug in {
                            "true",
                            "false",
                        }, f"{self.slug}: Checkbox must have 'true' and 'false' choices"
                    dimensions_view[dim["slug"]] = choice_slug

            # Get config (remainder)
            config = {
                k: v
                for k, v in block.items()
                if k
                not in {
                    "tableSlug",
                    "ySlugs",
                    "xSlug",
                    "colorSlug",
                    "sizeSlug",
                    *dimension_slug_to_raw_name.values(),
                }
            }

            views.append(
                {
                    "dimensions": dimensions_view,
                    "indicators": indicators,
                    "config": config,
                }
            )

        return {
            "config": self.config,
            "dimensions": dimensions,
            "views": views,
        }

    def _postprocess_config(self, config: dict) -> dict:
        """Process the configuration object by converting string values to appropriate Python types
        and cleaning up the config dictionary.

        This method:
        1. Converts string values like 'true', 'false', 'null', and numeric strings to their
           respective Python types (bool, None, int, float)
        2. Removes any key-value pairs with None values from the 'config' key to avoid
           cluttering the output with null values
        """
        config = _convert_strings_to_types(config)

        for k, v in list(config["config"].items()):
            if v is None:
                del config["config"][k]

        return config

    def run(self):
        if self.types == {"csv"}:
            config = self.run_csv()
        elif self.types == {"indicator"}:
            raise NotSupportedException("Not supported. Soon will be.")
        else:
            raise NotSupportedException("Not supported")

        return self._postprocess_config(config)


class NotSupportedException(Exception):
    pass


class TableURLNotInCataloException(Exception):
    pass


def _extract_table_uri(catalog_url: str):
    match = re.fullmatch(PATTERN_CSV, catalog_url)

    if match:
        extracted_fragment = match.group("group")
    else:
        raise TableURLNotInCataloException(f"{catalog_url}")

    # Don't keep full path like `explorers/who/latest/flu/flu`, but only keep the table name
    extracted_fragment = extracted_fragment.split("/")[-1]

    return f"{extracted_fragment}"


def _convert_strings_to_types(config: Any) -> Any:
    """Recursively process data structures to convert string representations
    of booleans ('true', 'false'), null ('null', 'None'), and numbers
    to their Python equivalents (True, False, None, int, float).

    This function handles nested dictionaries and lists, processing each element
    to ensure proper type conversion throughout the entire data structure.
    """
    if isinstance(config, dict):
        return {k: _convert_strings_to_types(v) for k, v in config.items()}
    elif isinstance(config, list):
        return [_convert_strings_to_types(item) for item in config]
    elif isinstance(config, str):
        # Convert string representations to actual values
        if config.lower() == "true":
            return True
        elif config.lower() == "false":
            return False
        elif config.lower() in ("null", "none"):
            return None
        else:
            # Try to convert to numeric types
            try:
                # First try to convert to int
                int_val = int(config)
                # If successful and the string representation matches the int,
                # it's a proper integer, not a float like "1.0"
                if str(int_val) == config:
                    return int_val

                # Otherwise try float
                return float(config)
            except ValueError:
                # If conversion fails, return the original string
                return config
    else:
        # Return other types (int, float, bool, None) as is
        return config


def _get_explorer_config(owid_env: OWIDEnv, name: str) -> Dict[str, Any]:
    # Load explorer from DB
    with Session(owid_env.engine) as session:
        db_exp = gm.Explorer.load_explorer(session, slug=name)
        if db_exp is None:
            raise ValueError(f"Explorer '{name}' not found in the database.")
    return db_exp.config


def reorder_dimensions(config: Dict[str, Any], dim_order: List[str]) -> Dict[str, Any]:
    """Reorder dimensions in the config dictionary."""
    config = deepcopy(config)
    config["dimensions"] = [[dim for dim in config["dimensions"] if dim["slug"] == slug][0] for slug in dim_order]
    return config


def migrate_csv_explorer(name: str, owid_env: OWIDEnv | None = None):
    """Migrate the TSV-based config of a CSV-based explorer to the new format.

    Note:
        - Only works for CSV-based explorers which use ETL data (i.e. have a catalog URL for all tables)
        - The output config is not fully functional yet. It might use a catalog path from a table that is not in Grapher (e.g. an 'explorer' table). Modify it so it points to a table in Grapher.

    Local path to explorer.
    """
    # Load explorer from DB
    explorer_config = _get_explorer_config(owid_env or OWID_ENV, name)

    # Create ExplorerMigration object
    migration = ExplorerMigration(explorer_config, name)

    if migration.types != {"csv"}:
        raise ValueError(f"{name}: Not a CSV explorer")

    config = migration.run()

    return config
