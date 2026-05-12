"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot("flu_vaccine_policy.xlsx")
    tb = snap.read_excel()

    # Drop the trailing summary row.
    tb = tb[:-1]

    # Drop columns we won't use, rename to canonical names.
    tb = tb.drop(columns=["ISO_3_CODE", "WHO_REGION", "INDCODE", "INDCAT_DESCRIPTION", "INDSORT"])
    tb = tb.rename(columns={"COUNTRYNAME": "country", "YEAR": "year"})

    # Long → wide on DESCRIPTION; each indicator becomes its own column.
    tb = tb.pivot(
        index=["country", "year"], columns="DESCRIPTION", values="VALUE", join_column_levels_with="_"
    )

    tb = tb.format(["country", "year"], short_name=paths.short_name)
    tb.update_metadata_from_yaml(paths.metadata_path, "flu_vaccine_policy")

    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
