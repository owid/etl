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
    snap_years_education = paths.load_snapshot("years_of_education.xlsx")
    snap_years_education_gini = paths.load_snapshot("years_of_education_gini.xlsx")
    snap_years_education_gender = paths.load_snapshot("years_of_education_gender.xlsx")
    snap_numeracy = paths.load_snapshot("numeracy.xlsx")
    snap_numeracy_gender = paths.load_snapshot("numeracy_gender.xlsx")
    #
    # Process data.
    #
    tbs = []

    for snap in [
        snap_years_education,
        snap_years_education_gini,
        snap_years_education_gender,
        snap_numeracy,
        snap_numeracy_gender,
    ]:
        tb = snap.read_excel()
        # Melting the table.
        year_cols = [str(year) for year in range(1500, 2051)]
        tb_melted = tb.melt(
            id_vars=["country name"],
            value_vars=year_cols,
            var_name="year",
            value_name=snap.metadata.short_name,
        )
        tbs.append(tb_melted)

    merged_tb = tbs[0]
    # Iterate through the remaining tables and merge them.
    for tb in tbs[1:]:
        merged_tb = pr.merge(merged_tb, tb, on=["country name", "year"], how="outer")
    merged_tb = merged_tb.rename(columns={"country name": "country"})

    # Ensure all columns are snake-case.
    tb = merged_tb.underscore()
    tb.metadata.short_name = paths.short_name
    tb = tb.set_index(["country", "year"], verify_integrity=True)
    tb = tb.dropna(how="all")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=None)

    # Save changes in the new garden dataset.
    ds_meadow.save()
