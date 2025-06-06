"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import map_series

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

# Define label for missing data.
# NOTE: This is a label used by us (which currently coincides with the one used in the original data).
NO_DATA_LABEL = "NO DATA"

# Define label for having no active farms reported.
# This will be used in countries with no active farms, where there is either no ban, or no information about a ban.
NO_ACTIVE_FARMS_REPORTED_LABEL = "No active farms reported"

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
    # As requested by the Fur-Free Alliance, we will replace "NO DATA" by "No active farms reported", since, in this case, there is no sign of fur farming for those countries.
    # NOTE: The same does not apply to fur trading.
    NO_ACTIVE_FARMS_REPORTED_LABEL: NO_ACTIVE_FARMS_REPORTED_LABEL,
    # NOTE: Some countries (for now Argentina and Brazil) have no ban, but it's unclear if they have active farms, so they will be labeled as "NO DATA".
    NO_DATA_LABEL: NO_DATA_LABEL,
}
# Define label for fur farming status that are not yet effective.
# NOTE: For consistency, ensure definitions are like in bullfighting_laws and chick_culling_laws datasets.
BANNED_NOT_EFFECTIVE = "Banned but not yet in effect"

# Rename fur trading status:
FUR_TRADING_BAN_STATUS = {
    "YES": "Banned",
    "PARTIAL": "Partially banned",
    "": "Not banned",
    NO_DATA_LABEL: NO_DATA_LABEL,
}

# Rename fur farming activity status:
FUR_FARMING_ACTIVE = {
    "YES": "Yes",
    "NO": "No",
    # There is a new status called "NO DATA". We will replace it by nan.
    "NO DATA": NO_DATA_LABEL,
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("fur_laws")
    tb = ds_meadow.read("fur_laws")

    # Load regions dataset and read its main table.
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions.read("regions")

    #
    # Process data.
    #
    # Select relevant columns and rename them.
    tb = tb[COLUMNS.keys()].rename(columns=COLUMNS, errors="raise")

    # Remove empty rows.
    tb = tb.dropna(how="all").reset_index(drop=True)

    # Harmonize country names.
    tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)

    # Add all countries that are not in the data, assuming they have no active fur farms.
    tb_added = (
        tb_regions[
            (~tb_regions["name"].isin(tb["country"].unique()))
            & (tb_regions["region_type"] == "country")
            & (tb_regions["defined_by"] == "owid")
        ][["name"]]
        .assign(
            **{
                "fur_farms_active": "NO",
                "fur_farming_status": NO_ACTIVE_FARMS_REPORTED_LABEL,
                "fur_trading_status": NO_DATA_LABEL,
            }
        )
        .rename(columns={"name": "country"}, errors="raise")
    )
    tb = pr.concat([tb, tb_added], ignore_index=True)

    # Keep only years.
    tb["ban_effective_year"] = tb["ban_effective_date"].str.extract(r"(\d{4})")
    tb["ban_effective_year"] = tb["ban_effective_year"].copy_metadata(tb["ban_effective_date"])
    tb = tb.drop(columns=["ban_effective_date"], errors="raise")

    # Add a current year column.
    tb["year"] = CURRENT_YEAR

    # Prepare fur farming ban status.
    tb = prepare_fur_farming_ban_status(tb=tb)

    # Prepare fur trading ban statuses.
    tb["fur_trading_status"] = map_series(
        tb["fur_trading_status"].astype("string").fillna(""),
        mapping=FUR_TRADING_BAN_STATUS,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
        show_full_warning=True,
    )

    # Prepare fur farming activity statuses.
    tb["fur_farms_active"] = map_series(
        tb["fur_farms_active"].astype("string").fillna(""),
        mapping=FUR_FARMING_ACTIVE,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
        show_full_warning=True,
    )

    # Sanity check.
    assert tb[tb.duplicated(subset="country", keep=False)].empty, "Duplicated rows found."

    # Run sanity checks.
    run_sanity_checks(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()


def prepare_fur_farming_ban_status(tb: Table) -> Table:
    tb = tb.copy()
    # Fill missing values with "".
    tb["fur_farming_status"] = tb["fur_farming_status"].astype("string").fillna("")

    # There is a column for the status of the ban, and another for those cases where there is no ban, but fur farming
    # has been phased out due to stricter regulations.
    # Check that when phase out is "YES", the ban status is empty.
    ####################################################################################################################
    # For Belgium, both columns "Fur farming ban" and "Phase-out due to stricter regulations" are "YES".
    # In this case, assume the stronger status, which is banned.
    # NOTE: I confirmed this with Fur Free Alliance.
    error = (
        "Expected Belgium to have both a fur farming ban and a phase out due to stricter regulations. "
        "This known data issue is no longer there, so it may have been fixed. Remove this part of the code."
    )
    assert tb.loc[tb["country"] == "Belgium", "fur_farming_status"].item() == "YES", error
    assert tb.loc[tb["country"] == "Belgium", "phase_out_due_to_stricter_regulations"].item() == "YES", error
    tb.loc[tb["country"] == "Belgium", "phase_out_due_to_stricter_regulations"] = None
    ####################################################################################################################
    error = "There are rows where phase out is 'YES' but the ban status was not empty."
    assert tb[(tb["phase_out_due_to_stricter_regulations"] == "YES") & (tb["fur_farming_status"] != "")].empty, error

    # Fill those nans in ban status with the new status.
    tb.loc[
        (tb["phase_out_due_to_stricter_regulations"] == "YES") & (tb["fur_farming_status"] == ""), "fur_farming_status"
    ] = PHASE_OUT_DUE_TO_STRICTER_REGULATIONS

    # Drop unnecessary column.
    tb = tb.drop(columns=["phase_out_due_to_stricter_regulations"], errors="raise")

    # There are countries (for now, Argentina and Brazil) that have no ban, but it's unclear if they have active farms (the column for operating fur farms says "NO DATA").
    # Ensure they appear as "NO DATA".
    select_unclear = tb["fur_farms_active"] == NO_DATA_LABEL
    tb.loc[select_unclear, "fur_farming_status"] = NO_DATA_LABEL

    # Map all fur farming statuses.
    tb["fur_farming_status"] = map_series(
        tb["fur_farming_status"],
        mapping=FUR_FARMING_BAN_STATUS,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
        show_full_warning=True,
    )

    # Some countries (for now, only Cyprus) has no ban, but no active farms. Also Israel has no data on a ban, but no active farms.
    # In both cases, it should appears as no active farms reported.
    tb.loc[(tb["fur_farming_status"] == "Not banned") & (tb["fur_farms_active"] == "NO"), "fur_farming_status"] = (
        NO_ACTIVE_FARMS_REPORTED_LABEL
    )

    # For those years years that are in the future, change the status.
    tb.loc[tb["ban_effective_year"].astype(float) > CURRENT_YEAR, "fur_farming_status"] = BANNED_NOT_EFFECTIVE

    return tb


def run_sanity_checks(tb: Table) -> None:
    error = "There were unknown fur farmed statuses."
    assert tb[tb["fur_farming_status"].isna()].empty, error

    error = "There were unknown fur trading statuses."
    assert tb[tb["fur_trading_status"].isna()].empty, error

    # Ensure all columns are informed (except the year of ban enforcement).
    error = "There were missing values in some columns."
    assert tb.drop(columns="ban_effective_year").isna().sum().sum() == 0, error
