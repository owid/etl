"""Load a meadow dataset and create a garden dataset."""

from shared import add_dim_some_education, make_table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("wittgenstein_human_capital_proj")

    # Read table from meadow dataset.
    paths.log.info("reading tables...")
    tb = ds_meadow.read("main").reset_index(drop=True)
    tb_age = ds_meadow.read("by_age").reset_index(drop=True)
    tb_sex = ds_meadow.read("by_sex").reset_index(drop=True)
    tb_edu = ds_meadow.read("by_edu").reset_index(
        drop=True
    )  # TODO: add metadata field for table explaining the different education levels
    tb_sex_age = ds_meadow.read("by_sex_age").reset_index(drop=True)
    tb_age_edu = ds_meadow.read("by_age_edu").reset_index(drop=True)
    tb_sex_age_edu = ds_meadow.read("by_sex_age_edu").reset_index(drop=True)

    #
    # Process data.
    #

    # 1/ MAKE MAIN TABLE
    paths.log.info("baking main table...")
    tb = make_table(
        tb,
        country_mapping_path=paths.country_mapping_path,
        cols_single=["tdr", "ggapmys25", "mage", "ydr", "ggapmys15", "odr"],
        cols_range=["growth", "imm", "emi", "cbr", "nirate", "cdr"],
        per_100=["tdr", "odr", "ydr"],
        per_1000=["emi", "imm"],
    )

    # 2.1/ MAKE BY AGE TABLE (sexratio)
    paths.log.info("baking tables by age...")
    tb_age = make_table(
        tb_age,
        country_mapping_path=paths.country_mapping_path,
        all_single=True,
        per_100=["sexratio"],
    )

    # 2.2/ BY SEX
    paths.log.info("baking tables by sex...")
    tb_sex = make_table(
        tb_sex,
        country_mapping_path=paths.country_mapping_path,
        all_range=True,
    )

    # 2.3/ BY EDU
    paths.log.info("baking tables by education...")
    tb_edu = make_table(
        tb_edu,
        country_mapping_path=paths.country_mapping_path,
        cols_single=["ggapedu15", "ggapedu25"],
        cols_range=["macb", "tfr", "net"],
        per_1000=["net"],
    )

    # 3.1/ BY SEX+AGE
    paths.log.info("baking tables by sex+age...")
    tb_sex_age = make_table(
        tb_sex_age,
        country_mapping_path=paths.country_mapping_path,
        all_single=True,
    )

    # 3.2/ BY AGE+EDU
    paths.log.info("baking tables by age+education...")
    assert "total" not in set(tb_age_edu["age"].unique()), "Unexpected age category: 'total'"
    tb_age_edu = make_table(
        tb_age_edu,
        country_mapping_path=paths.country_mapping_path,
        all_range=True,
    )

    # 4.1/ BY SEX+AGE+EDU
    paths.log.info("baking tables by sex+age+education...")
    tb_sex_age_edu = make_table(
        tb_sex_age_edu,
        country_mapping_path=paths.country_mapping_path,
        dtypes={
            "sex": "category",
            "age": "category",
            "education": "category",
        },
        cols_single=["pop", "prop"],
        cols_range=["assr"],
        per_1000=["pop"],
        per_100=["assr"],
    )

    # Add education="some_education" (only for sex=total and age=total, and indicator 'pop')
    tb_sex_age_edu = add_dim_some_education(tb_sex_age_edu)

    #
    # Save outputs.
    #
    # Format
    tables = [
        tb.format(["country", "year", "scenario"], short_name="main"),
        tb_age.format(["country", "scenario", "age", "year"], short_name="by_age"),
        tb_sex.format(["country", "scenario", "sex", "year"], short_name="by_sex"),
        tb_edu.format(["country", "scenario", "education", "year"], short_name="by_edu"),
        tb_sex_age.format(["country", "scenario", "sex", "age", "year"], short_name="by_sex_age"),
        tb_age_edu.format(["country", "scenario", "age", "education", "year"], short_name="by_age_edu"),
        tb_sex_age_edu.format(["country", "scenario", "sex", "age", "education", "year"], short_name="by_sex_age_edu"),
    ]
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
