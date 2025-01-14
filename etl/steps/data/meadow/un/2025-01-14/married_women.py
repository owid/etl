"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("married_women.xlsx")

    # Load data from snapshot.
    tb_countries = snap.read(sheet_name="Countries", skiprows=5)
    tb_regions = snap.read(sheet_name="Regions", skiprows=5)

    # Select relevant columns
    tb_regions = tb_regions[
        ["Region and subregion", "Year", "Regional Classification", "AgeGroup", "Percentage", "Number", "DataProcess"]
    ]

    tb_countries = tb_countries[["Country or area", "Year", "AgeGroup", "Percentage", "Number", "DataProcess"]]
    #
    # Process data.
    #
    # Rename columns
    tb_countries = tb_countries.rename(
        columns={
            "Country or area": "country",
            "Percentage": "percentage_married_union",
            "Number": "number_married_union",
        }
    )
    tb_regions = tb_regions.rename(
        columns={
            "Region and subregion": "country",
            "Percentage": "percentage_married_union",
            "Number": "number_married_union",
        }
    )

    # Filter out rows for Australia and New Zealand where regional classification is SDG-M49 (same as SDG)
    tb_regions = tb_regions[
        ~(
            (tb_regions["country"].isin(["Australia and New Zealand"]))
            & (tb_regions["Regional Classification"] == "SDG-M49")
        )
    ]
    tb_regions = tb_regions.drop(columns=["Regional Classification"])

    tb = pr.concat([tb_countries, tb_regions], axis=0)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [tb.format(["country", "year", "agegroup", "dataprocess"])]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()
