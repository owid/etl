"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define index columns
INDEX_SECTORS = ["donor_name", "recipient_name", "year", "sector_name"]
INDEX_CHANNELS = ["donor_name", "recipient_name", "year", "channel_name", "parent_channel_code"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("official_development_assistance_one")

    # Read tables from meadow dataset.
    tb_sectors = ds_meadow["sectors"].reset_index()
    tb_channels = ds_meadow["channels"].reset_index()

    #
    # Process data.
    #
    tb_sectors = geo.harmonize_countries(
        df=tb_sectors,
        country_col="donor_name",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )
    tb_sectors = geo.harmonize_countries(
        df=tb_sectors,
        country_col="recipient_name",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )

    tb_channels = geo.harmonize_countries(
        df=tb_channels,
        country_col="donor_name",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )
    tb_channels = geo.harmonize_countries(
        df=tb_channels,
        country_col="recipient_name",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=False,
        warn_on_unknown_excluded_countries=False,
    )

    # Create donor-only tables (using recipient_name = "All recipients (OECD)") and recipient-only tables (using donor_name = "Official donors (OECD)").
    tb_sectors_donor = tb_sectors[tb_sectors["recipient_name"] == "All recipients (OECD)"].reset_index(drop=True)
    tb_sectors_recipient = tb_sectors[tb_sectors["donor_name"] == "Official donors (OECD)"].reset_index(drop=True)

    tb_channels_donor = tb_channels[tb_channels["recipient_name"] == "All recipients (OECD)"].reset_index(drop=True)
    tb_channels_recipient = tb_channels[tb_channels["donor_name"] == "Official donors (OECD)"].reset_index(drop=True)

    # Format tables.
    tb_sectors = tb_sectors.format(INDEX_SECTORS, short_name="sectors")
    tb_sectors_donor = tb_sectors_donor.format(INDEX_SECTORS, short_name="sectors_donor")
    tb_sectors_recipient = tb_sectors_recipient.format(INDEX_SECTORS, short_name="sectors_recipient")

    tb_channels = tb_channels.format(INDEX_CHANNELS, short_name="channels")
    tb_channels_donor = tb_channels_donor.format(INDEX_CHANNELS, short_name="channels_donor")
    tb_channels_recipient = tb_channels_recipient.format(INDEX_CHANNELS, short_name="channels_recipient")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[
            tb_sectors,
            tb_sectors_donor,
            tb_sectors_recipient,
            tb_channels,
            tb_channels_donor,
            tb_channels_recipient,
        ],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
