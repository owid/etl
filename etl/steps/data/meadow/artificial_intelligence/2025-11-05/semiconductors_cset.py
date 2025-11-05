"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    paths.log.info("semiconductors_cset.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot for provision data (provision.csv).
    snap = paths.load_snapshot("semiconductors_cset.csv")

    # Retrieve snapshot for provider mappings.
    snap_providers = paths.load_snapshot("semiconductors_cset_providers.csv")

    # Read snapshots
    tb = snap.read()
    tb_providers = snap_providers.read()

    #
    # Process data.
    #
    # Keep relevant columns from provision data
    columns_to_keep = ["provider_name", "provided_name", "share_provided", "year"]
    tb = tb[columns_to_keep]

    # Fix provider name mismatches based on technical notes
    # "Various" in provision.csv should map to "Various countries" in providers.csv
    tb["provider_name"] = tb["provider_name"].replace(
        {
            "Various": "Various countries",
            # Add other known name changes from technical notes
            "EMD Performance Materials": "EMD Electronics",
            "Haute Gas": "Huate Gas",
            "Showa Denka": "Showa Denko",
            "Jinhong": "Jinhong Gas",
            "Quik-Pak": "QP Technologies",
            "Zhonghuan": "Zhonghuan Semiconductor",
            "Fujifilm": "Fujifilm Electronic Materials",
        }
    )

    columns_to_keep_providers = ["provider_name", "alias", "provider_type", "country"]
    tb_providers = tb_providers[columns_to_keep_providers]

    # Merge with providers mapping to get country and provider type
    tb = pr.merge(
        tb,
        tb_providers,
        on="provider_name",
        how="left",
    )

    # Log any providers that didn't match
    unmatched = tb[tb["provider_type"].isna()]["provider_name"].unique()
    if len(unmatched) > 0:
        paths.log.warning(f"Found {len(unmatched)} providers without mapping: {sorted(unmatched)[:10]}")

    # Create display_name: use alias if it exists (for countries), otherwise use provider_name
    tb["display_name"] = tb["alias"].fillna(tb["provider_name"])

    # For countries (provider_type = 'country'), the country column should be the display_name
    # For organizations, use the country column from the mapping
    tb["country_col"] = tb.apply(
        lambda row: row["display_name"]
        if pd.notna(row["provider_type"]) and row["provider_type"] == "country"
        else row["country"],
        axis=1,
    )

    # Convert share_provided to numeric
    tb["share_provided"] = pd.to_numeric(tb["share_provided"], errors="coerce")

    # Drop rows with missing share_provided values
    tb = tb.dropna(subset=["share_provided"])

    # Ensure year is integer
    tb["year"] = tb["year"].astype(int)

    # Keep only the columns we need
    tb = tb[["display_name", "country_col", "provided_name", "share_provided", "year"]]

    # Rename columns for clarity
    tb = tb.rename(columns={"display_name": "provider", "country_col": "country"})

    # Sort by year, provider, and provided name
    tb = tb.sort_values(["year", "provider", "provided_name"])

    # Reset index
    tb = tb.reset_index(drop=True)

    paths.log.info(f"Loaded {len(tb)} rows with {len(tb['provider'].unique())} unique providers")

    #
    # Create a new table.
    #
    tb = tb.format(["provider", "provided_name", "year"])

    # Add origins metadata to columns that don't have it
    for col in ["country", "share_provided"]:
        if col in tb.columns:
            tb[col].metadata.origins = snap.metadata.origin

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

    paths.log.info("semiconductors_cset.end")
