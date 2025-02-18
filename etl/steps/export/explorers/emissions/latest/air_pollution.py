"""Load a grapher dataset and create an explorer dataset with its tsv file."""

import pandas as pd
from structlog import get_logger

from etl.helpers import PathFinder, create_explorer

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load air pollutants grapher dataset and read its main table.
    ds = paths.load_dataset("ceds_air_pollutants")
    tb = ds.read("ceds_air_pollutants")

    #
    # Process data.
    #
    # Prepare graphers table of explorer.
    variable_ids = []
    pollutant_dropdown = []
    sector_dropdown = []
    per_capita_checkbox = []
    map_tab = []
    # Auxiliary list of pollutants as they appear in column names.
    pollutant_short_names = []
    for column in tb.drop(columns=["country", "year"]).columns:
        dimensions = tb[column].metadata.additional_info["dimensions"]
        if dimensions["originalShortName"] == "emissions":
            per_capita = False
        elif dimensions["originalShortName"] == "emissions_per_capita":
            per_capita = True
        else:
            raise ValueError(f"Unknown emissions type for column: {column}")

        for filter in dimensions["filters"]:
            if filter["name"] == "pollutant":
                pollutant_short_name = filter["value"]
                pollutant = f'{tb[column].metadata.display["name"]} ({filter["value"]})'
            elif filter["name"] == "sector":
                sector = filter["value"]
            else:
                raise ValueError(f"Unknown filter type for column: {column}")

        # Append extracted values.
        variable_ids.append([f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}"])
        pollutant_dropdown.append(pollutant)
        sector_dropdown.append(sector)
        per_capita_checkbox.append(per_capita)
        map_tab.append(True)
        pollutant_short_names.append(pollutant_short_name)

    # Create graphers table.
    df_graphers = pd.DataFrame()
    df_graphers["yVariableIds"] = variable_ids
    df_graphers["Pollutant Dropdown"] = pollutant_dropdown
    df_graphers["Sector Dropdown"] = sector_dropdown
    df_graphers["Per capita Checkbox"] = per_capita_checkbox
    df_graphers["hasMapTab"] = map_tab

    # Sanity check.
    error = "Duplicated rows in explorer."
    assert df_graphers[
        df_graphers.duplicated(subset=["Pollutant Dropdown", "Sector Dropdown", "Per capita Checkbox"], keep=False)
    ].empty, error

    # Create view for all pollutants.
    # Omit CH4 and N20 in this view.
    for per_capita in [False, True]:
        _columns = [
            f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}"
            for column in tb.columns
            if column.endswith("all_sectors")
            if (("per_capita" in column) == per_capita)
            if "_ch4_" not in column
            if "_n2o_" not in column
        ]
        df_graphers = pd.concat(
            [
                df_graphers,
                pd.DataFrame(
                    {
                        "yVariableIds": [_columns],
                        "Pollutant Dropdown": "All pollutants",
                        "Sector Dropdown": "All sectors",
                        "Per capita Checkbox": per_capita,
                        "hasMapTab": False,
                        "selectedFacetStrategy": "metric",
                        "facetYDomain": "independent",
                    }
                ),
            ]
        )

    # Create breakdown by sector.
    for pollutant, pollutant_short_name in zip(pollutant_dropdown, pollutant_short_names):
        for per_capita in [False, True]:
            _columns_for_pollutant = []
            for column in tb.drop(columns=["country", "year"]).columns:
                dimensions = tb[column].metadata.additional_info["dimensions"]
                if (not per_capita and dimensions["originalShortName"] == "emissions") or (
                    per_capita and dimensions["originalShortName"] == "emissions_per_capita"
                ):
                    for filter in dimensions["filters"]:
                        if (filter["name"] == "pollutant") and (filter["value"] == pollutant_short_name):
                            if not column.endswith("all_sectors"):
                                _columns_for_pollutant.append(f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}")
            if len(_columns_for_pollutant) == 0:
                continue
            df_graphers = pd.concat(
                [
                    df_graphers,
                    pd.DataFrame(
                        {
                            "yVariableIds": [_columns_for_pollutant],
                            "Pollutant Dropdown": pollutant,
                            "Sector Dropdown": "Breakdown by sector",
                            "Per capita Checkbox": per_capita,
                            "hasMapTab": False,
                            "selectedFacetStrategy": "entity",
                            "facetYDomain": "independent",
                        }
                    ),
                ]
            )

    # Impose that all line charts start at zero.
    df_graphers["yAxisMin"] = 0

    # Sort rows conveniently.
    df_graphers = df_graphers.sort_values(["Pollutant Dropdown", "Sector Dropdown", "Per capita Checkbox"]).reset_index(
        drop=True
    )

    # Choose which indicator to show by default when opening the explorer.
    df_graphers["defaultView"] = False
    df_graphers.loc[
        (df_graphers["Pollutant Dropdown"] == "All pollutants")
        & (df_graphers["Sector Dropdown"] == "All sectors")
        & (~df_graphers["Per capita Checkbox"]),
        "defaultView",
    ] = True

    # Prepare explorer metadata.
    config = {
        "name": "air-pollution",
        "explorerTitle": "Air Pollution",
        "explorerSubtitle": "Explore historical emissions of air pollutants across the world.",
        "selection": ["China", "India", "United Kingdom", "United States", "World"],
        "isPublished": False,
    }

    #
    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.
    ds_explorer = create_explorer(dest_dir=dest_dir, config=config, df_graphers=df_graphers)
    ds_explorer.save()
