"""Load a grapher dataset and create an explorer dataset with its tsv file."""

import pandas as pd
from owid.catalog.utils import underscore
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize log.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Label to use for the breakdown by sector in the sector dropdown.
BREAKDOWN_BY_SECTOR_LABEL = "Breakdown by sector"

# Old and new labels the all sectors in the sector dropdown.
# NOTE: The old label should coincide with the one used in the garden step.
ALL_SECTORS_LABEL_OLD = "All sectors"
ALL_SECTORS_LABEL_NEW = "All sectors (total)"

# Label to use for all pollutants in the pollutants dropdown.
ALL_POLLUTANTS_LABEL = "All pollutants"

POLLUTANT_NAME = {
    "NH₃": "Ammonia",
    "BC": "Black carbon",
    "CO": "Carbon monoxide",
    "CH₄": "Methane",
    "NOₓ": "Nitrogen oxides",
    "N₂O": "Nitrous oxide",
    "NMVOC": "Non-methane volatile organic compounds",
    "OC": "Organic carbon",
    "SO₂": "Sulfur dioxide",
}


def run() -> None:
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
                pollutant = POLLUTANT_NAME[pollutant_short_name]
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

    # Create view for all pollutants.
    # NOTE: Here, we could create a vew not only for "All sectors" but also for each individual sector. But there is a technical problem: The display name of each indicator would be the same, and therefore the faceted view of all pollutants for a given sector would show the same name (the sector name) on top of each small chart. To achieve this, we would probably need to create duplicates of indicators, with a different set of display names.
    # Also note that having this dropdown with all sectors as options (which doesn't contain a "Breakdown by sector") causes that, for any other pollutant view (e.g. "Agriculture") the Sector dropdown will show "Breakdown by sector" at the bottom. This happens because "All pollutants" is the first option in the Pollutants dropdown, which therefore sets the order of sectors in the sectors dropdown.
    # So, for now, we'll keep only one option for "Sector Dropdown" when "All pollutants" is selected.
    # for sector in sorted(set(df_graphers["Sector Dropdown"])):
    for sector in [ALL_SECTORS_LABEL_OLD]:
        sector_short_name = sector.lower().replace(" ", "_")
        for per_capita in [False, True]:
            _columns = []
            for pollutant_short_name in [underscore(key) for key in POLLUTANT_NAME.keys()]:
                column = [
                    column
                    for column in tb.columns
                    if (f"_{pollutant_short_name}_" in column)
                    and (("per_capita" in column) == per_capita)
                    and (sector_short_name in column)
                ]
                if len(column) == 1:
                    _columns.append(f"{ds.metadata.uri}/{tb.metadata.short_name}#{column[0]}")
            df_graphers = pd.concat(
                [
                    df_graphers,
                    pd.DataFrame(
                        {
                            "yVariableIds": [_columns],
                            "title": f"Per capita emissions of air pollutants from {sector.lower()}"
                            if per_capita
                            else f"Emissions of air pollutants from {sector.lower()}",
                            "subtitle": "Measured in kilograms and split by major pollutant."
                            if per_capita
                            else "Measured in tonnes and split by major pollutant.",
                            "Pollutant Dropdown": ALL_POLLUTANTS_LABEL,
                            "Sector Dropdown": sector,
                            "Per capita Checkbox": per_capita,
                            "hasMapTab": False,
                            "selectedFacetStrategy": "metric",
                            "facetYDomain": "independent",
                        }
                    ),
                ]
            )

    # Create breakdown by sector.
    for pollutant, pollutant_short_name in list(dict.fromkeys(zip(pollutant_dropdown, pollutant_short_names))):
        for per_capita in [False, True]:
            _columns_for_pollutant = []
            _pollutant_title = []
            _pollutant_subtitle = []
            for column in tb.drop(columns=["country", "year"]).columns:
                dimensions = tb[column].metadata.additional_info["dimensions"]
                if (not per_capita and dimensions["originalShortName"] == "emissions") or (
                    per_capita and dimensions["originalShortName"] == "emissions_per_capita"
                ):
                    for filter in dimensions["filters"]:
                        if (filter["name"] == "pollutant") and (filter["value"] == pollutant_short_name):
                            if not column.endswith("all_sectors"):
                                _columns_for_pollutant.append(f"{ds.metadata.uri}/{tb.metadata.short_name}#{column}")
                                _pollutant_title.append(POLLUTANT_NAME[pollutant_short_name])
                                _pollutant_subtitle.append(tb[column].metadata.description_short)
            if len(_columns_for_pollutant) == 0:
                continue
            assert len(set(_pollutant_title)) == 1, "Multiple pollutants with the same title."
            assert len(set(_pollutant_subtitle)) == 1, "Multiple pollutants with the same description."
            title = (
                f"Per capita {_pollutant_title[0].lower()} emissions by sector"
                if per_capita
                else f"{_pollutant_title[0]} emissions by sector"
            )
            subtitle = _pollutant_subtitle[0]
            df_graphers = pd.concat(
                [
                    df_graphers,
                    pd.DataFrame(
                        {
                            "yVariableIds": [_columns_for_pollutant],
                            "title": title,
                            "subtitle": subtitle,
                            "Pollutant Dropdown": pollutant,
                            "Sector Dropdown": BREAKDOWN_BY_SECTOR_LABEL,
                            "Per capita Checkbox": per_capita,
                            "hasMapTab": False,
                            "selectedFacetStrategy": "entity",
                            "facetYDomain": "independent",
                        }
                    ),
                ]
            )

    # Rename all sectors label in sector dropdown.
    df_graphers["Sector Dropdown"] = df_graphers["Sector Dropdown"].replace(
        ALL_SECTORS_LABEL_OLD, ALL_SECTORS_LABEL_NEW
    )

    # Sanity check.
    error = "Duplicated rows in explorer."
    assert df_graphers[
        df_graphers.duplicated(subset=["Pollutant Dropdown", "Sector Dropdown", "Per capita Checkbox"], keep=False)
    ].empty, error

    # Impose that all line charts start at zero.
    df_graphers["yAxisMin"] = 0

    # Choose which indicator to show by default when opening the explorer.
    df_graphers["defaultView"] = False
    df_graphers.loc[
        (df_graphers["Pollutant Dropdown"] == ALL_POLLUTANTS_LABEL)
        & (df_graphers["Sector Dropdown"] == ALL_SECTORS_LABEL_NEW)
        & (~df_graphers["Per capita Checkbox"]),
        "defaultView",
    ] = True

    # Sort rows conveniently.
    sector_categories = [ALL_SECTORS_LABEL_NEW, BREAKDOWN_BY_SECTOR_LABEL] + sorted(
        set(df_graphers["Sector Dropdown"]) - {ALL_SECTORS_LABEL_NEW, BREAKDOWN_BY_SECTOR_LABEL}
    )
    df_graphers["Sector Dropdown"] = pd.Categorical(
        df_graphers["Sector Dropdown"],
        categories=sector_categories,
        ordered=True,
    )
    df_graphers = df_graphers.sort_values(["Pollutant Dropdown", "Sector Dropdown", "Per capita Checkbox"]).reset_index(
        drop=True
    )

    # Prepare explorer metadata.
    config = {
        "name": "air-pollution",
        "explorerTitle": "Air Pollution",
        "explorerSubtitle": "Explore historical emissions of air pollutants across the world.",
        "selection": ["China", "India", "United Kingdom", "United States", "World"],
        "yScaleToggle": True,
        "isPublished": True,
    }

    #
    # Save outputs.
    #
    # Create a new explorers dataset and tsv file.
    ds_explorer = paths.create_explorer(config=config, df_graphers=df_graphers)
    ds_explorer.save()
