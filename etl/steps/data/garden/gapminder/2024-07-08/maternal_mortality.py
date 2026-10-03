import numpy as np

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("maternal_mortality")

    # Read table from meadow dataset.
    tb = ds_meadow["maternal_mortality"].reset_index()

    # replace year ranges with mid year and convert year to Int64
    tb["year"] = tb["year"].apply(lambda x: get_mid_year_from_year_str(x)).astype("Int64")

    # rename column
    tb = tb.rename(columns={"women_reproductive_age__15_49": "number_of_women_between_15_49_years"})

    # harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def get_mid_year_from_year_str(year_str: str) -> int:
    """Get the mid year from a year string or range of years.
    year_str: str - year string or string describing a range of years"""
    if "-" in year_str:
        start, end = year_str.split("-")
        start_int = int(start)
        end_int = int(end)
        # if two full four digit years - nothing to do
        if len(start) == 4 and len(end) == 4:
            return int((start_int + end_int) // 2)
        # one full four digit year and last two digits of end year
        elif len(start) == 4 and len(end) == 2:
            start_century = np.floor(start_int / 100) * 100
            start_year = start_int % 100
            if start_year <= end_int:
                end_int = start_century + end_int
            elif start_year > end_int:
                end_int = start_century + 100 + end_int
        # one full four digit year and last digit of end year
        elif len(start) == 4 and len(end) == 1:
            start_decade = np.floor(start_int / 10) * 10
            start_year = start_int % 10
            if start_year <= end_int:
                end_int = start_decade + end_int
            elif start_year > end_int:
                end_int = start_decade + 10 + end_int
        else:
            raise ValueError(f"Invalid year range: {year_str}")
        return int((start_int + end_int) // 2)
    else:
        return int(year_str)
