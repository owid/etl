"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    ds_garden = paths.load_dataset("education_attainment_distribution")
    aggregates_to_exclude = ["European Union (25 countries)", "G20", "OECD"]

    tb = ds_garden["education_attainment_distribution"].reset_index()
    tb = tb[~tb["country"].isin(aggregates_to_exclude)]
    tb = tb.format(["country", "year"])

    tb_oecd = ds_garden["education_attainment_distribution_oecd"].reset_index()
    tb_oecd = tb_oecd[~tb_oecd["country"].isin(aggregates_to_exclude)]
    tb_oecd = tb_oecd.format(["country", "year"])

    tb_wc = ds_garden["education_attainment_distribution_wc"].reset_index()
    tb_wc = tb_wc.format(["country", "year"])

    tb_wc_no_edu = ds_garden["education_no_formal_wc"].reset_index()
    tb_wc_no_edu = tb_wc_no_edu.format(["country", "year"])

    tb_wc_some_edu = ds_garden["education_some_formal_wc"].reset_index()
    tb_wc_some_edu = tb_wc_some_edu.format(["country", "year"])

    tb_wc_no_edu_sex = ds_garden["education_no_formal_by_sex_wc"].reset_index()
    tb_wc_no_edu_sex = tb_wc_no_edu_sex.format(["country", "year", "sex"])

    ds_grapher = paths.create_dataset(
        tables=[tb, tb_oecd, tb_wc, tb_wc_no_edu, tb_wc_some_edu, tb_wc_no_edu_sex], default_metadata=ds_garden.metadata
    )
    ds_grapher.save()
