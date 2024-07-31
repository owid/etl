"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("google_mobility")

    # Read table from meadow dataset.
    tb = ds_meadow["google_mobility"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Remove subnational data, keeping only country figures
    tb = keep_national(tb)
    tb = rename_columns(tb)
    tb = smooth_indicators(tb)
    tb = unpivot(tb)

    # Format
    tb = tb.format(["country", "date", "place"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def keep_national(tb: Table) -> Table:
    filter_cols = [
        "sub_region_1",
        "sub_region_2",
        "metro_area",
        "iso_3166_2_code",
        "census_fips_code",
    ]
    tb = tb[tb[filter_cols].isna().all(1)]

    tb = tb.drop(columns=filter_cols)
    return tb


def rename_columns(tb: Table) -> Table:
    rename_dict = {
        "retail_and_recreation_percent_change_from_baseline": "retail_and_recreation",
        "grocery_and_pharmacy_percent_change_from_baseline": "grocery_and_pharmacy",
        "parks_percent_change_from_baseline": "parks",
        "transit_stations_percent_change_from_baseline": "transit_stations",
        "workplaces_percent_change_from_baseline": "workplaces",
        "residential_percent_change_from_baseline": "residential",
    }

    # Rename columns
    tb = tb.rename(columns=rename_dict)
    return tb


def smooth_indicators(tb: Table) -> Table:
    tb = tb.sort_values(by=["country", "date"]).reset_index(drop=True)
    smoothed_cols = [
        "retail_and_recreation",
        "grocery_and_pharmacy",
        "parks",
        "transit_stations",
        "workplaces",
        "residential",
    ]
    tb[smoothed_cols] = (
        tb.groupby("country", observed=True)[smoothed_cols]
        .rolling(window=7, min_periods=3, center=False)
        .mean()
        .reset_index(level=0, drop=True)
    )
    tb[smoothed_cols] = tb[smoothed_cols].round(3)

    return tb


def unpivot(tb: Table) -> Table:
    tb = tb.melt(["country", "date"], var_name="place", value_name="trend")
    tb["place"] = tb["place"].replace(
        {
            "retail_and_recreation": "Retail and recreation",
            "grocery_and_pharmacy": "Grocery and pharmacy",
            "parks": "Parks",
            "transit_stations": "Transit stations",
            "workplaces": "Workplaces",
            "residential": "Residential",
        }
    )
    return tb
