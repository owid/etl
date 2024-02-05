"""Load a garden dataset and create a grapher dataset.

"""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its monthly table.
    ds_garden = paths.load_dataset("climate_change_impacts")
    tb_monthly = ds_garden["climate_change_impacts_monthly"].reset_index()

    #
    # Process data.
    #
    # Create country, year and month columns (required by grapher).
    tb_monthly = tb_monthly.rename(columns={"location": "country"}, errors="raise")
    tb_monthly["year"] = tb_monthly["date"].astype(str).str.split("-").str[0].astype(int)
    tb_monthly["month"] = tb_monthly["date"].astype(str).str.split("-").str[1].astype(int)


    # Set an appropriate index and sort conveniently.
    tb_monthly = tb_monthly.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Fix metadata to be able to work in grapher.
    for column in tb_monthly.columns:
        if not tb_monthly[column].metadata.display:
            tb_monthly[column].metadata.display = {}
        tb_monthly[column].metadata.display["yearIsDay"] = True

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_monthly], check_variables_metadata=True)
    ds_grapher.save()
