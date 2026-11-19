"""Build long-run series of deaths and death rates by cause in the United States, from 1900 to the latest year.

Two sources are combined:
- NCHS's historical tables (HIST290 and HIST290A) for 1900–1998, published as one table per revision period of the
  International Classification of Diseases (ICD). Each period names its causes differently, so each cause we
  publish is mapped to the matching cause name in each period.
- CDC WONDER for 1999 onward, using NCHS's list of 113 selected causes of death (ICD-10).

Before 1933 the data covers only the "death-registration states", not the whole country.

Two tables come out of this step:
- `causes_of_death`: deaths and crude death rates for a selection of causes.
- `broad_cause_groups`: deaths and the share of all deaths in four groups that add up to all deaths.
"""

import re

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# NCHS periods (one per ICD revision period, as named in the meadow table).
PERIODS = ["1900-1939", "1940-1949", "1950-1959", "1960-1967", "1968-1978", "1979-1998"]

# Each published cause -> how it is named in each NCHS period, and in CDC WONDER (regular expressions, which must
# match exactly one cause name). Causes that NCHS starts reporting later have no entry for the earlier periods.
CAUSES = {
    "All causes": {
        **{period: r"^ALL CAUSES$" for period in PERIODS},
        "wonder": r"^All Causes of Death$",
    },
    "Heart disease": {
        "1900-1939": r"^DISEASES OF THE HEART$",
        "1940-1949": r"^DISEASES OF HEART$",
        "1950-1959": r"^DISEASES OF HEART \(ICD 400-402, 410-413\)$",
        "1960-1967": r"^ICD-7 Nos\. 400-402, 410-416 Diseases of heart$",
        "1968-1978": r"^390-398, 404, 410-429 Diseases of heart$",
        "1979-1998": r"^DISEASES OF HEART \(390-398,402,404-429\)$",
        "wonder": r"^#Diseases of heart \(",
    },
    "Cancer": {
        "1900-1939": r"^MALIGNANT NEOPLASMS$",
        "1940-1949": r"^MALIGNANT NEOPLASMS$",
        "1950-1959": r"^MALIGNANT NEOPLASMS \(ICD NOS\. 140-205\)$",
        "1960-1967": r"^ICD-7 Nos\. 140-205 Malignant neoplasms$",
        "1968-1978": r"^MALIGNANT NEOPLASMS INCLUDING NEOPLASMS OF LYMPHATIC AND HEMATOPOIETIC TISSUES \(140-209\)$",
        "1979-1998": r"^MALIGNANT NEOPLASMS, INCLUDING NEOPLASMS OF LYMPHATIC AND HEMATOPOIETIC TISSUES \(140-208\)$",
        "wonder": r"^#Malignant neoplasms \(C00-C97\)$",
    },
    "Stroke": {
        "1900-1939": r"^VASCULAR LESIONS AFFECTING CENTRAL NERVOUS SYSTEM$",
        "1940-1949": r"^VASCULAR LESIONS AFFECTING CENTRAL NERVOUS SYSTEM$",
        "1950-1959": r"^CEREBROVASCULAR DISEASES \(ICD 330-334\)$",
        "1960-1967": r"^ICD-7 Nos\. 330-334 Cerebrovascular diseases$",
        "1968-1978": r"^Cerebrovascular disease 430-438$",
        "1979-1998": r"^CEREBROVASCULAR DISEASES \(430-438\)$",
        "wonder": r"^#Cerebrovascular diseases \(I60-I69\)$",
    },
    "Pneumonia and influenza": {
        "1900-1939": r"^INFLUENZA AND PNEUMONIA$",
        "1940-1949": r"^INFLUENZA AND PNEUMONIA, EXCEPT PNEUMONIA OF NEWBORN$",
        "1950-1959": r"^INFLUENZA AND PNEUMONIA, EXCEPT PNEUMONIA OF NEWBORN \(480-493\)$",
        "1960-1967": r"^ICD-7 Nos\. 480-493 Pneumonia and influenza$",
        "1968-1978": r"^PNEUMONIA AND INFLUENZA 470-474, 480-486$",
        "1979-1998": r"^PNEUMONIA AND INFLUENZA \(480-487\)$",
        "wonder": r"^#Influenza and pneumonia \(J09-J18\)$",
    },
    "Tuberculosis": {
        "1900-1939": r"^TUBERCULOSIS, ALL FORMS$",
        "1940-1949": r"^TUBERCULOSIS, ALL FORMS$",
        "1950-1959": r"^TUBERCULOSIS, ALL FORMS \(001-019\)$",
        "1960-1967": r"^Tuberculosis, all forms \(001-019\)$",
        "1968-1978": r"^TUBERCULOSIS, ALL FORMS \(010-019\)$",
        "1979-1998": r"^TUBERCULOSIS \(010-018\)$",
        "wonder": r"^#Tuberculosis \(A16-A19\)$",
    },
    "Accidents": {
        "1900-1939": r"^ACCIDENTS$",
        "1940-1949": r"^ACCIDENTS$",
        "1950-1959": r"^ACCIDENTS AND ADVERSE EFFECTS \(E800-E962\)$",
        "1960-1967": r"^ICD-7 Nos\. E800-E962 Accidents$",
        "1968-1978": r"^E800-E949- ACCIDENTS$",
        "1979-1998": r"^ACCIDENTS AND ADVERSE EFFECTS \(E800-E949\)$",
        "wonder": r"^#Accidents \(unintentional injuries\) \(V01-X59,Y85-Y86\)$",
    },
    "Road accidents": {
        "1900-1939": r"^MOTOR.VEHICLE ACCIDENTS$",
        "1940-1949": r"^MOTOR.VEHICLE ACCIDENTS$",
        "1950-1959": r"^MOTOR VEHICLE ACCIDENTS \(E810-E835\)$",
        "1960-1967": r"^Motor vehicle accidents \(E810-E835\)$",
        "1968-1978": r"^MOTOR VEHICLE ACCIDENTS \(E810-E823\)$",
        "1979-1998": r"^MOTOR VEHICLE ACCIDENTS \(E810-E825\)$",
        "wonder": r"^Motor vehicle accidents \(V02-V04,",
    },
    "Diabetes": {
        "1900-1939": r"^DIABETES MELLITUS$",
        "1940-1949": r"^DIABETES MELLITUS$",
        "1950-1959": r"^DIABETES \(260\)$",
        "1960-1967": r"^ICD-7 Nos\. 260 Diabetes$",
        "1968-1978": r"^DIABETES MELLITUS \(250\)$",
        "1979-1998": r"^DIABETES MELLITUS \(250\)$",
        "wonder": r"^#Diabetes mellitus \(E10-E14\)$",
    },
    "Suicide": {
        "1900-1939": r"^SUICIDE$",
        "1940-1949": r"^SUICIDE$",
        "1950-1959": r"^SUICIDE \(E963, E970-E979\)$",
        "1960-1967": r"^ICD-7 Nos\. E963-E979 Suicide$",
        "1968-1978": r"^Suicide E950-E959$",
        "1979-1998": r"^SUICIDE \(E950-E959\)$",
        "wonder": r"^#Intentional self-harm \(suicide\) \(",
    },
    "Diarrheal diseases": {
        "1900-1939": r"^GASTRITIS, DUODENITIS, ENTERITIS AND COLITIS, EXCEPT DIARRHEA OF NEWBORN$",
        "1940-1949": r"^GASTRITIS, DUODENITIS, ENTERITIS, AND COLITIS$",
        "1950-1959": r"^GASTRITIS, DUODENITIS, ENTERITIS, AND COLITIS, EXCEPT DIARRHEA OF NEWBORN \(543, 571, 572\)$",
        "1960-1967": r"^Gastritis, duodenitis, enteritis, and colitis, except diarrhea of newborn \(543,571,572\)$",
        "1968-1978": r"^ENTERITIS AND OTHER DIARRHEAL DISEASES \(008,009\)$",
        "1979-1998": r"^CERTAIN OTHER INTESTINAL INFECTIONS \(007-009\)$",
        "wonder": r"^Certain other intestinal infections \(A04,A07-A09\)$",
    },
    # NCHS's series of chronic obstructive pulmonary disease (COPD) starts in 1960; from 1999 it is called "chronic
    # lower respiratory diseases".
    "COPD": {
        "1960-1967": r"^ICD-7 Nos\. 241, 501, 502, 527\.1 Chronic obstructive pulmonary diseases",
        "1968-1978": r"^Chronic obstructive pulmonary diseases and allied conditions 490-493",
        "1979-1998": r"^CHRONIC OBSTRUCTIVE PULMONARY DISEASES AND ALLIED CONDITIONS \(490-496\)$",
        "wonder": r"^#Chronic lower respiratory diseases \(J40-J47\)$",
    },
    # Alzheimer's disease and COVID-19 are only available from CDC WONDER.
    "Alzheimer’s disease": {"wonder": r"^#Alzheimer disease \(G30\)$"},
    "COVID-19": {"wonder": r"^#COVID-19 \(U07\.1\)$"},
}

# First year of each cause that starts later than 1900 (COVID-19: WONDER reports zero deaths before 2020).
FIRST_YEAR = {"COPD": 1960, "Alzheimer’s disease": 1999, "COVID-19": 2020}

# Four groups that add up to all deaths: three sums of the causes above, and the rest.
GROUPS = {
    "Heart disease, cancer, stroke, and diabetes": ["Heart disease", "Cancer", "Stroke", "Diabetes"],
    "Tuberculosis, pneumonia, influenza, and diarrheal diseases": [
        "Tuberculosis",
        "Pneumonia and influenza",
        "Diarrheal diseases",
    ],
    "Accidents": ["Accidents"],
}
GROUP_OTHER = "All other causes"


def run() -> None:
    #
    # Load inputs.
    #
    tb_nchs = paths.load_dataset("nchs_historical_mortality_tables").read(
        "nchs_historical_mortality_tables", safe_types=False
    )
    tb_wonder = paths.load_dataset("cdc_wonder_deaths_by_cause").read("cdc_wonder_deaths_by_cause", safe_types=False)
    sanity_check_inputs(tb_nchs, tb_wonder)

    #
    # Process data.
    #
    # Select and rename the causes we publish, in both sources.
    tb_nchs = select_nchs_causes(tb_nchs)
    tb_wonder = select_wonder_causes(tb_wonder)

    # Combine the two sources: NCHS until 1998, CDC WONDER from 1999.
    tb = pr.concat([tb_nchs, tb_wonder], ignore_index=True)

    # Drop the years before each late-starting cause is reported.
    for cause, first_year in FIRST_YEAR.items():
        tb = tb[~((tb["cause"] == cause) & (tb["year"] < first_year))]

    # Add the four broad groups.
    tb_groups = make_broad_groups(tb)

    tb["country"] = "United States"
    tb_groups["country"] = "United States"

    sanity_check_outputs(tb, tb_groups)

    tb = tb.format(["country", "year", "cause"], short_name="causes_of_death")
    tb_groups = tb_groups.format(["country", "year", "cause_group"], short_name="broad_cause_groups")

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb, tb_groups])
    ds_garden.save()


def match_one(names: list[str], pattern: str, where: str) -> str:
    """Return the only name matching a pattern, and fail if there is none or several."""
    matches = [name for name in names if re.search(pattern, name)]
    assert len(matches) == 1, f"{where}: pattern {pattern!r} matches {matches}."
    return matches[0]


def select_nchs_causes(tb: Table) -> Table:
    """Keep the published causes from the NCHS tables, as one row per cause and year with deaths and rates."""
    tb = tb.astype({"cause": str, "period": str, "measure": str})
    # The rates and deaths files of a period can spell a name with different hyphen characters.
    tb["cause"] = tb["cause"].str.replace("\u2010", "-", regex=False)
    selected = []
    for cause, names in CAUSES.items():
        for period, pattern in names.items():
            if period == "wonder":
                continue
            tb_period = tb[tb["period"] == period]
            name = match_one(sorted(set(tb_period["cause"])), pattern, where=f"{cause}, {period}")
            selected.append(tb_period[tb_period["cause"] == name].assign(cause=cause))
    tb = pr.concat(selected, ignore_index=True)

    # Put deaths and death rates side by side.
    tb = tb.pivot(index=["cause", "year"], columns="measure", values="value").reset_index()
    return tb[["cause", "year", "deaths", "death_rate"]]


def select_wonder_causes(tb: Table) -> Table:
    """Keep the published causes from CDC WONDER, with their deaths and crude rates."""
    tb = tb.reset_index().astype({"cause": str})
    names = sorted(set(tb["cause"]))
    selected = []
    for cause, patterns in CAUSES.items():
        name = match_one(names, patterns["wonder"], where=f"{cause}, WONDER")
        selected.append(tb[tb["cause"] == name].assign(cause=cause))
    tb = pr.concat(selected, ignore_index=True)
    return tb.rename(columns={"crude_rate": "death_rate"})[["cause", "year", "deaths", "death_rate"]]


def make_broad_groups(tb: Table) -> Table:
    """Sum deaths into four groups that add up to all deaths, and compute each group's share of all deaths."""
    membership = {cause: group for group, causes in GROUPS.items() for cause in causes}

    # Sum the component causes of each group (all components must be present, or the group would be too small).
    tb_parts = tb[tb["cause"].isin(list(membership))].copy()
    tb_parts["cause_group"] = tb_parts["cause"].map(membership)
    n_parts = tb_parts.dropna(subset=["deaths"]).groupby(["year", "cause_group"])["cause"].nunique()
    n_expected = n_parts.index.get_level_values("cause_group").map({g: len(c) for g, c in GROUPS.items()})
    assert (n_parts.values == n_expected.values).all(), "A group is missing deaths for one of its causes."
    tb_groups = tb_parts.groupby(["year", "cause_group"], as_index=False)["deaths"].sum()

    # Add the remainder: all deaths minus the three groups.
    tb_all = tb[tb["cause"] == "All causes"][["year", "deaths"]].rename(columns={"deaths": "all_deaths"})
    tb_grouped = tb_groups.groupby("year", as_index=False)["deaths"].sum().rename(columns={"deaths": "grouped"})
    tb_other = pr.merge(tb_all, tb_grouped, on="year", how="inner")
    tb_other["deaths"] = tb_other["all_deaths"] - tb_other["grouped"]
    tb_other["cause_group"] = GROUP_OTHER
    tb_groups = pr.concat([tb_groups, tb_other[["year", "cause_group", "deaths"]]], ignore_index=True)

    # Share of all deaths in each group.
    tb_groups = pr.merge(tb_groups, tb_all, on="year", how="left")
    tb_groups["share_of_deaths"] = 100 * tb_groups["deaths"] / tb_groups["all_deaths"]
    return tb_groups.drop(columns=["all_deaths"])


def sanity_check_inputs(tb_nchs: Table, tb_wonder: Table) -> None:
    periods = set(tb_nchs.reset_index()["period"].astype(str))
    assert periods == set(PERIODS), f"Unexpected NCHS periods: {periods}"
    years_wonder = set(tb_wonder.reset_index()["year"])
    assert min(years_wonder) == 1999, "CDC WONDER data should start in 1999."
    assert set(range(1999, max(years_wonder) + 1)) == years_wonder, "Gap in the CDC WONDER years."


def sanity_check_outputs(tb: Table, tb_groups: Table) -> None:
    last_year = int(tb["year"].max())

    # Every cause covers every year from its first year to the latest one, with no duplicates.
    assert not tb.duplicated(subset=["cause", "year"]).any(), "Duplicate (cause, year) rows."
    for cause, group in tb.groupby("cause"):
        first_year = FIRST_YEAR.get(cause, 1900)
        if cause == "Road accidents":
            # NCHS reports motor-vehicle deaths separately from 1906.
            first_year = 1906
        expected = set(range(first_year, last_year + 1))
        years = set(group["year"])
        assert years == expected, f"{cause}: missing years {sorted(expected - years)[:10]}"
        assert group["death_rate"].notna().all(), f"{cause}: missing death rates."

    # Deaths are known for every cause and year, except COPD in 1960–1968: the 1960–67 deaths table (HIST290A) has
    # no COPD row, and the 1968–78 one leaves 1968 blank. The death rates (HIST290) cover those years.
    missing_deaths = tb[tb["deaths"].isna()][["cause", "year"]].values.tolist()
    assert missing_deaths == [["COPD", year] for year in range(1960, 1969)], (
        f"Unexpected missing deaths: {missing_deaths}"
    )

    # No cause exceeds all causes, in deaths or in rates.
    wide = tb.pivot(index="year", columns="cause", values="death_rate")
    parts = wide.drop(columns="All causes")
    assert (parts.le(wide["All causes"], axis=0) | parts.isna()).all().all(), "A cause's rate exceeds all causes."

    # Deaths and rates agree: the population implied by each cause's deaths and rate matches the one implied by all
    # causes, within the rounding of the rates (one decimal), for every cause and year with at least 20 deaths.
    population = 1e5 * tb[tb["cause"] == "All causes"].set_index("year")["deaths"] / wide["All causes"]
    tb_check = tb[(tb["deaths"] >= 20) & (tb["cause"] != "All causes")].copy()
    tb_check["implied_rate"] = 1e5 * tb_check["deaths"] / tb_check["year"].map(population)
    error = (tb_check["implied_rate"] - tb_check["death_rate"]).abs()
    tolerance = 0.06 + 0.002 * tb_check["death_rate"]
    bad = tb_check[error > tolerance]
    assert bad.empty, f"Deaths and death rates disagree: {bad[['cause', 'year', 'deaths', 'death_rate']].head()}"

    # The four groups add up to all deaths, and the remainder is positive.
    shares = tb_groups.pivot(index="year", columns="cause_group", values="share_of_deaths")
    assert ((shares.sum(axis=1) - 100).abs() < 1e-6).all(), "Group shares don't add up to 100%."
    assert (tb_groups["deaths"] > 0).all(), "A broad group has zero or negative deaths."
    assert set(tb_groups["year"]) == set(range(1900, last_year + 1)), "Broad groups miss some years."
