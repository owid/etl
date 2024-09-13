"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Get current year from this step's version.
CURRENT_YEAR = int(paths.version.split("-")[0])

# Columns to use and how to rename them.
COLUMNS = {
    "country": "country",
    "fur_farming_ban": "fur_farming_status",
    "phase_out_due_to_stricter_regulations": "phase_out_due_to_stricter_regulations",
    "fur_trade_ban": "fur_trading_status",
    "effective": "ban_effective_date",
    "operating_fur_farms": "fur_farms_active",
}

# Define status for having no bans, but having phased out fur farming due to stricter regulations.
PHASE_OUT_DUE_TO_STRICTER_REGULATIONS = "Phased out due to stricter regulations"
# Rename fur farming status:
FUR_FARMING_BAN_STATUS = {
    "YES": "Banned",
    "NO": "Not banned",
    "PARTIAL": "Partially banned",
    "Parliamentary debate": "Not banned",
    "": "Not banned",
    PHASE_OUT_DUE_TO_STRICTER_REGULATIONS: PHASE_OUT_DUE_TO_STRICTER_REGULATIONS,
}
# Define label for fur farming status that are not yet effective.
BANNED_NOT_EFFECTIVE = "Banned (not yet effective)"

# Rename fur trading status:
FUR_TRADING_BAN_STATUS = {
    "YES": "Banned",
    "PARTIAL": "Partially banned",
    "": "Not banned",
}

# Rename fur farming activity status:
FUR_FARMING_ACTIVE = {
    "YES": "Yes",
    "NO": "No",
    # NOTE: We assume that if there is no information, then there are no fur farms.
    # This is the case of Montenegro (Fur Free Alliance confirmed this case).
    "": "No",
}


def prepare_fur_farming_ban_status(tb: Table) -> Table:
    tb = tb.copy()
    # Fill missing values with "" and strip spaces.
    tb["fur_farming_status"] = tb["fur_farming_status"].astype(object).fillna("").str.strip()

    # Rename fur farming ban statuses.
    # Find all status that start with "YES" and map them to "YES".
    tb.loc[tb["fur_farming_status"].str.startswith("YES"), "fur_farming_status"] = "YES"
    # Find all status that start with "PARTIAL" and map them to "PARTIAL".
    tb.loc[tb["fur_farming_status"].str.startswith("PARTIAL"), "fur_farming_status"] = "PARTIAL"

    # There is a column for the status of the ban, and another for those cases where there is no ban, but fur farming
    # has been phased out due to stricter regulations.
    # Check that when phase out is "YES", the ban status is empty.
    error = "There are rows where phase out is 'YES' but the ban status was not empty."
    assert tb[(tb["phase_out_due_to_stricter_regulations"] == "YES") & (tb["fur_farming_status"] != "")].empty, error

    # Fill those nans in ban status with the new status.
    tb.loc[
        (tb["phase_out_due_to_stricter_regulations"] == "YES") & (tb["fur_farming_status"] == ""), "fur_farming_status"
    ] = PHASE_OUT_DUE_TO_STRICTER_REGULATIONS

    # Drop unnecessary column.
    tb = tb.drop(columns=["phase_out_due_to_stricter_regulations"])

    # Map all fur farming statuses.
    tb["fur_farming_status"] = tb["fur_farming_status"].map(FUR_FARMING_BAN_STATUS)

    # For those years years that are in the future, change the status.
    tb.loc[tb["ban_effective_year"].astype(float) > CURRENT_YEAR, "fur_farming_status"] = BANNED_NOT_EFFECTIVE

    return tb


def fix_inconsistencies(tb: Table) -> Table:
    tb = tb.copy()

    # China appears twice in the data, but with identical data.
    error = "Expected duplicate row for China; remove temporary fix."
    assert len(tb[(tb["country"] == "China") & (tb.duplicated(keep=False))]) == 2, error
    # Simply remove one of the two duplicate rows.
    tb = tb.drop_duplicates().reset_index(drop=True)

    # Denmark appears twice: Once with partial farming ban and no trading ban, and once with partial trading ban and no
    # farming ban.
    error = "Expected duplicate rows for Denmark; remove temporary fix."
    assert set(tb[(tb["country"] == "Denmark")]["fur_farming_status"]) == {"Not banned", "Partially banned"}
    # Impose partial ban on both farming and trading.
    tb.loc[tb["country"] == "Denmark", "fur_farming_status"] = "Partially banned"
    tb.loc[tb["country"] == "Denmark", "fur_trading_status"] = "Partially banned"
    # Drop duplicated rows.
    tb = tb.drop_duplicates().reset_index(drop=True)

    return tb


def run_sanity_checks(tb: Table) -> None:
    error = "There were unknown fur farmed statuses."
    assert tb[tb["fur_farming_status"].isna()].empty, error

    error = "There were unknown fur trading statuses."
    assert tb[tb["fur_trading_status"].isna()].empty, error

    # Ensure all columns are informed (except the year of ban enforcement).
    error = "There were missing values in some columns."
    assert tb.drop(columns="ban_effective_year").isna().sum().sum() == 0, error


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

    # Remove empty rows.
    tb = tb.dropna(how="all").reset_index(drop=True)

    # Harmonize country names.
    # Remove spurious spaces in country names.
    tb["country"] = tb["country"].str.strip()
    tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)

    # Keep only years.
    tb["ban_effective_year"] = tb["ban_effective_date"].str.extract(r"(\d{4})")
    tb["ban_effective_year"] = tb["ban_effective_year"].copy_metadata(tb["ban_effective_date"])
    tb = tb.drop(columns=["ban_effective_date"])

    # Add a current year column.
    tb["year"] = CURRENT_YEAR

    # Prepare fur farming ban status.
    tb = prepare_fur_farming_ban_status(tb=tb)

    # Prepare fur trading ban statuses.
    tb["fur_trading_status"] = tb["fur_trading_status"].astype(object).fillna("").map(FUR_TRADING_BAN_STATUS)

    # Prepare fur farming activity statuses.
    tb["fur_farms_active"] = tb["fur_farms_active"].astype(object).fillna("").map(FUR_FARMING_ACTIVE)

    # Fix inconsistent data points.
    tb = fix_inconsistencies(tb=tb)

    ####################################################################################################################
    # Manually fix some issues pointed out by the Fur Free Alliance (by email).
    # * Sweden should be labeled as "Partial" for fur farming ban.
    # * New Zealand should be labeled as "Partial" for fur farming ban.
    # First check that they are currently labeled as "Not banned" (in case it changes in an update).
    error = "Expected Sweden to be labeled as 'Not banned'; remove temporary fix."
    assert tb.loc[tb["country"] == "Sweden", "fur_farming_status"].item() == "Not banned", error
    tb.loc[tb["country"] == "Sweden", "fur_farming_status"] = "Partially banned"
    error = "Expected New Zealand to be labeled as 'Not banned'; remove temporary fix."
    assert tb.loc[tb["country"] == "New Zealand", "fur_farming_status"].item() == "Not banned"
    tb.loc[tb["country"] == "New Zealand", "fur_farming_status"] = "Partially banned"
    ####################################################################################################################

    # Run sanity checks.
    run_sanity_checks(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
