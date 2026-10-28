"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SUBJECTS = ["math", "science", "reading"]
# First PISA cycle with comparable results for each subject.
FIRST_YEAR = {"reading": 2000, "math": 2003, "science": 2006}
# The PISA 2025 report gives scores of girls and boys from this cycle onward; earlier ones come from the Data Explorer.
FIRST_YEAR_BY_GENDER = 2015
# Countries the PISA 2025 report marks with an asterisk, because they didn't meet one or more PISA sampling standards.
EXPECTED_FLAGGED_COUNTRIES = {"Albania", "Canada", "Netherlands", "New Zealand", "Norway", "United States"}
# The PISA 2025 report only gives the average of all OECD countries for some cycles; the others come from the Data
# Explorer, whose average is the same measure (both are 491.8 in reading in 2012, and 488.5 in 2015).
OECD_AVERAGE = "OECD average"
# The report gives these countries' PISA for Development results (an assessment held in 2017) under "PISA 2018".
# The meadow step checks this against the note under each trend table.
PISA_FOR_DEVELOPMENT = {"Cambodia", "Ecuador", "Guatemala", "Paraguay", "Zambia"}

# NOTE: The PISA 2025 report's trend tables show no value ("m", data removed for technical reasons) for the
# country-years below, although the PISA Data Explorer exports (up to PISA 2022) have one. We leave them out too, so
# that our series match the OECD's own comparisons over time. Except for Ukraine and Viet Nam, the 2025 report carries
# forward exclusions the OECD made in earlier cycles (the PISA 2022 Results, Volume I, already showed "m" for all of
# them). `check_values_omitted_by_report` fails when this list no longer matches the report, so check it on every
# update.
VALUES_OMITTED_BY_PISA_2025 = {
    # Argentina, Kazakhstan, Malaysia 2015: Argentina's sample didn't cover all eligible schools after a restructuring
    # of secondary schools; Kazakhstan's national coders marked too leniently, so the human-coded items were dropped;
    # Malaysia's school response rate (51%) fell well short of the 85% standard. PISA 2015 Results (Volume I),
    # Annex A4, p. 304: https://doi.org/10.1787/9789264266490-en
    ("Argentina", 2015): ("math", "science", "reading"),
    ("Kazakhstan", 2015): ("math", "science", "reading"),
    ("Malaysia", 2015): ("math", "science", "reading"),
    # Austria 2009: a teachers' union boycott affected how the test was administered and students' motivation, so the
    # results aren't comparable with other years. PISA 2009 Results (Volume I), Annex A4, p. 186:
    # https://doi.org/10.1787/9789264091450-en
    ("Austria", 2009): ("math", "science", "reading"),
    # Jordan, reading and science before 2022: when Jordan moved to computer-based tests in 2022, its reading and
    # science results could only be linked to the international scale at the expense of comparability over time, so
    # the OECD only reports trends in mathematics. PISA 2022 Results (Volume I), Annex A4, p. 282:
    # https://doi.org/10.1787/53f23881-en
    ("Jordan", 2006): ("science", "reading"),
    ("Jordan", 2009): ("science", "reading"),
    ("Jordan", 2012): ("science", "reading"),
    ("Jordan", 2015): ("science", "reading"),
    ("Jordan", 2018): ("science", "reading"),
    # Spain 2018: some regions held high-stakes exams at the end of the PISA testing window, and many students didn't
    # try their best, so comparability with other years "cannot be fully ensured". The OECD never published Spain's
    # 2018 reading results. PISA 2018 Results (Volume I), Annex A9 (updated 23 July 2020):
    # https://www.oecd.org/pisa/PISA2018-AnnexA9-Spain.pdf
    ("Spain", 2018): ("math", "science"),
    # Ukraine 2018: the report's Ukraine row covers only the 17 of 27 regions assessed in 2022 and 2025 (the others
    # weren't covered because of Russia's war against Ukraine), while PISA 2018 covered a larger part. PISA 2025
    # Results (Volume I), Reader's Guide: https://doi.org/10.1787/73451bc5-en
    ("Ukraine", 2018): ("math", "science", "reading"),
    # United Kingdom 2000 and 2003: the samples didn't meet PISA's response-rate standards. PISA 2012 Results
    # (Volume I), p. 53: https://doi.org/10.1787/9789264208780-en
    ("United Kingdom", 2000): ("reading",),
    ("United Kingdom", 2003): ("math", "reading"),
    # Viet Nam before 2025: it moved to computer-based tests in 2025, after older paper-based tests in all earlier
    # cycles, so the OECD leaves it out of trend analyses. PISA 2025 Results (Volume I), Reader's Guide:
    # https://doi.org/10.1787/73451bc5-en
    ("Vietnam", 2012): ("math", "science", "reading"),
    ("Vietnam", 2015): ("math", "science", "reading"),
    ("Vietnam", 2022): ("math", "science", "reading"),
}


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("pisa")
    # Scores from the PISA 2025 report, and from the PISA Data Explorer exports (up to PISA 2022).
    tb_report = ds_meadow.read("pisa")
    tb_explorer = ds_meadow.read("pisa_data_explorer")
    sanity_check_inputs(tb_report=tb_report, tb_explorer=tb_explorer)

    #
    # Process data.
    #
    # Step 1: Harmonize country names in both tables.
    tb_report = paths.regions.harmonize_names(tb_report, warn_on_unused_countries=False)
    tb_explorer = paths.regions.harmonize_names(tb_explorer, warn_on_unused_countries=False)
    tb_report = tb_report.drop(columns=["sampling_standards_not_met"])

    # Step 2: Date the PISA for Development results by the year they were collected.
    msk = tb_report["country"].isin(PISA_FOR_DEVELOPMENT) & (tb_report["year"] == 2018)
    assert set(tb_report.loc[msk, "country"]) == PISA_FOR_DEVELOPMENT, "PISA for Development rows not found."
    assert not (tb_explorer["country"].isin(PISA_FOR_DEVELOPMENT) & tb_explorer["year"].isin([2017, 2018])).any(), (
        "The Data Explorer also has PISA for Development years."
    )
    tb_report.loc[msk, "year"] = 2017

    # Step 3: Check which values the PISA 2025 report leaves out, and that they are the ones listed above.
    check_values_omitted_by_report(tb_report=tb_report, tb_explorer=tb_explorer)

    # Step 4: Combine both tables. The PISA 2025 report wins where both have a value (it includes revisions, e.g. the
    # Palestinian Authority's 2022 results, recalculated to match the 2025 coverage); the Data Explorer fills in the
    # rest (countries that didn't take part in PISA 2025, and girls and boys before 2015).
    tb_report["priority"] = 0
    tb_explorer["priority"] = 1
    tb = pr.concat([tb_report, tb_explorer], ignore_index=True).sort_values(["country", "year", "priority"])
    tb = tb.groupby(["country", "year"], as_index=False, observed=True).first().drop(columns=["priority"])

    # Step 5: Leave out the values the PISA 2025 report no longer shows (see the note above).
    for (country, year), subjects in VALUES_OMITTED_BY_PISA_2025.items():
        columns = [c for c in tb.columns if any(c.startswith(f"pisa_{subject}_") for subject in subjects)]
        tb.loc[(tb["country"] == country) & (tb["year"] == year), columns] = pd.NA

    # Step 6: Remove "_boys_girls" from the names of the columns for girls and boys.
    tb = tb.rename(columns=lambda column: column.replace("_boys_girls", ""))

    # Step 7: Add the bounds of the 95% confidence interval of the mean score of all students.
    for subject in SUBJECTS:
        average, se = tb[f"pisa_{subject}_all_average"], tb[f"pisa_{subject}_all_se"]
        tb[f"{subject}_lower_bound"] = average - 1.96 * se
        tb[f"{subject}_upper_bound"] = average + 1.96 * se

    # Step 8: Remove standard errors.
    tb = tb.drop(columns=[column for column in tb.columns if "_se" in column])

    # Step 9: Add metadata that depends on the subject and group of students.
    add_metadata(tb)

    sanity_check_outputs(tb)
    # NOTE: The table keeps the name of the previous version, which the pisa_performance MDim reads.
    tb = tb.format(["country", "year"], short_name="pisa_math_boys_girls")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def check_values_omitted_by_report(tb_report: Table, tb_explorer: Table) -> None:
    """Assert that the values the PISA 2025 report leaves out (but the Data Explorer has) are the ones listed above."""
    tb = pr.merge(tb_explorer, tb_report, on=["country", "year"], how="left", suffixes=("_explorer", "_report"))
    countries_in_report = set(tb_report["country"]) - {OECD_AVERAGE}
    omitted: dict[tuple[str, int], set[str]] = {}
    for subject in SUBJECTS:
        for column, first_year in [
            (f"pisa_{subject}_all_average", FIRST_YEAR[subject]),
            (f"pisa_{subject}_boys_girls_average_girls", FIRST_YEAR_BY_GENDER),
            (f"pisa_{subject}_boys_girls_average_boys", FIRST_YEAR_BY_GENDER),
        ]:
            mask = (
                tb[f"{column}_explorer"].notna()
                & tb[f"{column}_report"].isna()
                & tb["country"].isin(countries_in_report)
                & (tb["year"] >= first_year)
            )
            for country, year in zip(tb.loc[mask, "country"], tb.loc[mask, "year"]):
                omitted.setdefault((country, int(year)), set()).add(subject)
    expected = {key: set(subjects) for key, subjects in VALUES_OMITTED_BY_PISA_2025.items()}
    assert omitted == expected, (
        f"Values left out of the PISA 2025 report changed. "
        f"New: {sorted((k, sorted(v)) for k, v in omitted.items() if omitted.get(k) != expected.get(k))}. "
        f"No longer left out: {sorted((k, sorted(v)) for k, v in expected.items() if expected.get(k) != omitted.get(k))}."
    )


def add_metadata(tb: Table) -> None:
    """Add the descriptions that depend on each column's subject and group of students."""
    for column in tb.columns:
        if column in ["country", "year"]:
            continue
        if "math" in column:
            subject = "mathematics"
            description_short = "Assessed through the PISA mathematics scale, which measures how well someone can use math to solve everyday problems and understand the role of math in the real world."
        elif "reading" in column:
            subject = "reading"
            description_short = "Assessed through the PISA reading scale, which measures how well someone can understand and use written information to learn new things and be a part of society."
        else:
            subject = "science"
            description_short = "Assessed through the PISA science scale, which assesses how comfortable and knowledgeable someone is with science topics, focusing on their ability to discuss and think about scientific issues in everyday life."

        if "_girls" in column:
            students = "female students"
        elif "_boys" in column:
            students = "male students"
        else:
            students = "students"

        metadata = tb[column].metadata
        metadata.display = {"numDecimalPlaces": 0}
        metadata.description_short = description_short
        if "bound" not in column:
            metadata.description_from_producer = f"Average score of 15-year-old {students} on the PISA {subject} scale. Initially, the average PISA score across subjects and all OECD countries was at 500 with a standard deviation of 100, so that most students scored between 400 and 600. Scores in later cycles were calibrated to remain comparable to this baseline."
        else:
            bound_type = "upper" if "upper" in column else "lower"
            metadata.unit = "score"
            metadata.description_from_producer = f"Represents the {bound_type} bound of the average {subject} score for 15-year-old {students} on the PISA scale. Initially, the average PISA score across subjects and all OECD countries was at 500 with a standard deviation of 100, so that most students scored between 400 and 600. Scores in later cycles were calibrated to remain comparable to this baseline."
            metadata.title = (
                f"{bound_type.capitalize()} bound of performance of 15-year-old {students} on the {subject} scale"
            )
            metadata.description_processing = f"Calculated as the average {subject} score {'plus' if bound_type == 'upper' else 'minus'} 1.96 times the standard error, representing the {bound_type} bounds of a 95% confidence interval."
            metadata.processing_level = "major"
            metadata.display["name"] = f"{bound_type.capitalize()} bound {subject} score"


def sanity_check_inputs(tb_report: Table, tb_explorer: Table) -> None:
    expected = {"country", "year"}
    for subject in SUBJECTS:
        expected |= {f"pisa_{subject}_all_average", f"pisa_{subject}_all_se"}
        expected |= {
            f"pisa_{subject}_boys_girls_{stat}_{group}" for stat in ["average", "se"] for group in ["girls", "boys"]
        }
    assert set(tb_report.columns) == expected | {"sampling_standards_not_met"}, (
        "Unexpected columns in the report table."
    )
    assert set(tb_explorer.columns) == expected, "Unexpected columns in the Data Explorer table."
    flagged = set(tb_report.loc[tb_report["sampling_standards_not_met"], "country"])
    assert flagged == EXPECTED_FLAGGED_COUNTRIES, (
        f"Countries flagged for sampling standards changed: {sorted(flagged)}."
    )
    assert tb_report["year"].max() == 2025, "The report table should include PISA 2025."


def sanity_check_outputs(tb: Table) -> None:
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."
    averages = [column for column in tb.columns if "_average" in column]
    # PISA mean scores lie well within 200-700 (the OECD average was set to 500, with a standard deviation of 100).
    values = tb[averages]
    assert ((values.isna()) | ((values > 200) & (values < 700))).all().all(), "Mean scores outside 200-700."
    for subject in SUBJECTS:
        lower, upper, average = (
            tb[f"{subject}_lower_bound"],
            tb[f"{subject}_upper_bound"],
            tb[f"pisa_{subject}_all_average"],
        )
        assert ((average.isna()) | ((lower <= average) & (average <= upper))).all(), (
            f"Bounds don't contain the mean ({subject})."
        )
    # Coverage: about 90 countries and economies took part in PISA 2025.
    n_2025 = tb.loc[tb["year"] == 2025, "pisa_reading_all_average"].notna().sum()
    assert n_2025 >= 80, f"Only {n_2025} countries with reading scores in 2025."
