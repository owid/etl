"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Get current year from this step's version.
CURRENT_YEAR = int(paths.version.split("-")[0])

# Columns to use and how to rename them.
COLUMNS = {
    "country": "country",
    "fur_farming_ban__phase_out_due_to_stricter_regulations": "fur_farming_status",
    "fur_trade_ban": "fur_trading_status",
    "starting_date_effective_ban": "ban_effective_date",
    "operating_fur_farms": "fur_farms_active",
}

# Rename fur farming status:
FUR_FARMING_STATUS = {
    "YES": "Banned",
    "NO": "Not banned",
    "PARTIAL": "Partially banned",
    "Parliamentary debate": "Debated",
    "": "Not banned",
}
# Define label for fur farming status that are not yet effective.
BANNED_NOT_EFFECTIVE = "Banned (not yet effective)"

# Rename fur trading status:
FUR_TRADING_STATUS = {
    "YES": "Banned",
    "PARTIAL": "Partially banned",
    "": "Not banned",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("fur_laws")

    # Read table from meadow dataset.
    tb = ds_meadow["fur_laws"].reset_index()

    #
    # Process data.
    #
    # Select relevant columns and rename them.
    tb = tb[COLUMNS.keys()].rename(columns=COLUMNS, errors="raise")

    # Rename fur farming statuses.
    # Find all status that start with "YES" and map them to "YES".
    tb.loc[tb["fur_farming_status"].str.startswith("YES"), "fur_farming_status"] = "YES"
    # Find all status that start with "PARTIAL" and map them to "PARTIAL".
    tb.loc[tb["fur_farming_status"].str.startswith("PARTIAL"), "fur_farming_status"] = "PARTIAL"
    # Map all fur farming statuses.
    tb["fur_farming_status"] = tb["fur_farming_status"].map(FUR_FARMING_STATUS)
    error = "There were unknown fur farmed statuses."
    assert tb[tb["fur_farming_status"].isna()].empty, error

    # Rename fur trading statuses.
    tb["fur_trading_status"] = tb["fur_trading_status"].map(FUR_TRADING_STATUS)
    error = "There were unknown fur trading statuses."
    assert tb[tb["fur_trading_status"].isna()].empty, error

    # Keep only years.
    tb["ban_effective_year"] = tb["ban_effective_date"].str.extract(r"(\d{4})")
    tb["ban_effective_year"] = tb["ban_effective_year"].copy_metadata(tb["ban_effective_date"])
    tb = tb.drop(columns=["ban_effective_date"])

    # For those years years that are in the future, change the status.
    tb.loc[tb["ban_effective_year"].astype(float) > CURRENT_YEAR, "fur_farming_status"] = BANNED_NOT_EFFECTIVE

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
