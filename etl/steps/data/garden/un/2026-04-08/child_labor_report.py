"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

_NOT_IN_SCHOOL = "Children in child labour who are not attending school"
_BY_SECTOR = "Children in child labour by sector of economic activity"


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("child_labor_report")

    # Read tables from meadow dataset.
    tb_by_region = ds_meadow.read("child_labor_by_region")
    tb_trends = ds_meadow.read("child_labor_trends")
    tb_hazardous = ds_meadow.read("hazardous_work_by_region")

    #
    # Process data.
    #
    # Reshape region tables from wide to long (sex × age).
    tb_by_region = _to_long(tb_by_region)
    tb_hazardous = _to_long(tb_hazardous)
    tb_trends = _trends_to_long(tb_trends)

    # Split off "not in school" and "by sector" rows into separate tables.
    tb_by_region, tb_by_region_not_in_school, tb_by_region_sector = _split_table(tb_by_region, "child_labor")
    tb_hazardous, tb_hazardous_not_in_school, tb_hazardous_sector = _split_table(tb_hazardous, "hazardous_work")

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(
        tables=[
            tb_by_region,
            tb_by_region_not_in_school,
            tb_by_region_sector,
            tb_hazardous,
            tb_hazardous_not_in_school,
            tb_hazardous_sector,
            tb_trends,
        ],
        default_metadata=ds_meadow.metadata,
    )

    # Save garden dataset.
    ds_garden.save()


def _to_long(tb: Table) -> Table:
    """Reshape a wide region table into long format with sex and age dimensions.

    Columns like `total_5_11_pct` become rows with sex="total", age="5-11", and two
    value columns: `share` (from pct) and `number` (from no).
    """
    tb = tb.reset_index()

    # Unpivot all 24 value columns into a single variable/value pair.
    tb = tb.melt(id_vars=["region_type", "region"], var_name="variable", value_name="value")

    # Parse sex, age, and indicator from the column name pattern {sex}_{age}_{indicator}.
    parsed = tb["variable"].str.extract(r"^(total|boys|girls)_(\d+_\d+)_(pct|no)$")
    tb["sex"] = parsed[0]
    tb["age"] = parsed[1].str.replace("_", "-")
    tb["indicator"] = parsed[2]
    tb = tb.drop(columns="variable")

    # Drop rows that didn't match the pattern (e.g. from non-value columns).
    tb = tb.dropna(subset=["sex", "age", "indicator"])

    # Spread the indicator column back into two value columns (pct → share, no → number).
    tb = tb.pivot(index=["region_type", "region", "sex", "age"], columns="indicator", values="value")
    tb.columns.name = None
    tb = tb.rename(columns={"pct": "share", "no": "number"})
    tb = tb.reset_index()

    return tb.format(["region_type", "region", "sex", "age"])


def _trends_to_long(tb: Table) -> Table:
    """Reshape the trends table into long format with year as dimension.

    Columns like `child_labor_2016_pct` become year="2016" with four value columns:
    `share_child_labor`, `share_hazardous_work`, `number_child_labor`, `number_hazardous_work`.
    """
    tb = tb.reset_index()

    # Unpivot all 12 value columns into a single variable/value pair.
    tb = tb.melt(id_vars=["disaggregation_type", "disaggregation_value"], var_name="variable", value_name="value")

    # Parse indicator, year, and metric from the column name pattern {indicator}_{year}_{metric}.
    parsed = tb["variable"].str.extract(r"^(child_labor|hazardous_work)_(\d{4})_(pct|no)$")
    tb["indicator"] = parsed[0]
    tb["year"] = parsed[1]
    tb["metric"] = parsed[2]
    tb = tb.drop(columns="variable")

    # Drop rows that didn't match the pattern.
    tb = tb.dropna(subset=["indicator", "year", "metric"])

    # Combine metric and indicator into a single column name (e.g. share_child_labor, number_hazardous_work).
    tb["metric"] = tb["metric"].map({"pct": "share", "no": "number"})
    tb["col"] = tb["metric"] + "_" + tb["indicator"]
    tb = tb.drop(columns=["metric", "indicator"])

    # Spread into four value columns.
    tb = tb.pivot(index=["disaggregation_type", "disaggregation_value", "year"], columns="col", values="value")
    tb.columns.name = None
    tb = tb.reset_index()

    # Map disaggregation_type/value to country, sex, age.
    tb["country"] = tb["disaggregation_value"]
    tb["sex"] = "total"
    tb["age"] = "5-17"

    # World total: country="World".
    tb.loc[tb["disaggregation_type"] == "World total", "country"] = "World"

    # Sex rows: country="World", sex from value.
    is_sex = tb["disaggregation_type"] == "Sex"
    tb.loc[is_sex, "country"] = "World"
    tb.loc[is_sex, "sex"] = tb.loc[is_sex, "disaggregation_value"].str.lower()

    # Age rows: country="World", age from value (strip " years").
    is_age = tb["disaggregation_type"] == "Age"
    tb.loc[is_age, "country"] = "World"
    tb.loc[is_age, "age"] = tb.loc[is_age, "disaggregation_value"].str.replace(" years", "", regex=False)

    # Disambiguate regions that appear under multiple groupings (e.g. Sub-Saharan Africa).
    for label in ["ILO regions", "SDG regions", "UNICEF regions"]:
        suffix = label.split(" ")[0]  # "ILO", "SDG", "UNICEF"
        mask = tb["disaggregation_type"] == label
        tb.loc[mask, "country"] = tb.loc[mask, "country"] + f" ({suffix})"

    tb = tb.drop(columns=["disaggregation_type", "disaggregation_value"])

    return tb.format(["country", "year", "sex", "age"])


def _split_table(tb: Table, prefix: str) -> tuple[Table, Table, Table]:
    """Split a reshaped region table into three: regions, not-in-school, and by-sector.

    Returns (tb_regions, tb_not_in_school, tb_sector), each with a distinct short_name.
    """
    tb = tb.reset_index()

    # Filter rows for each category.
    tb_not_in_school = tb[tb["region_type"] == _NOT_IN_SCHOOL].reset_index(drop=True)
    tb_sector = tb[tb["region_type"] == _BY_SECTOR].reset_index(drop=True)
    tb = tb[~tb["region_type"].isin([_NOT_IN_SCHOOL, _BY_SECTOR])].reset_index(drop=True)

    # Sector table: drop region_type, rename region to sector, add country="World".
    tb_sector = tb_sector.drop(columns=["region_type"])
    tb_sector = tb_sector.rename(columns={"region": "sector"})
    tb_sector["country"] = "World"

    # Not-in-school table: drop region_type and region (always "Total"), add country="World".
    tb_not_in_school = tb_not_in_school.drop(columns=["region_type", "region"])
    tb_not_in_school["country"] = "World"

    # Regions table: append acronym from region_type, rename to country, set "World" for World total.
    acronym = {"ILO regions": "ILO", "SDG regions": "SDG", "UNICEF regions": "UNICEF"}
    tb.loc[tb["region_type"] == "World total", "region"] = "World"
    for label, suffix in acronym.items():
        mask = tb["region_type"] == label
        tb.loc[mask, "region"] = tb.loc[mask, "region"] + f" ({suffix})"
    tb = tb.drop(columns=["region_type"]).rename(columns={"region": "country"})

    # Re-format each table.
    tb = tb.format(["country", "sex", "age"], short_name=prefix)
    tb_not_in_school = tb_not_in_school.format(["country", "sex", "age"], short_name=f"{prefix}_not_in_school")
    tb_sector = tb_sector.format(["country", "sector", "sex", "age"], short_name=f"{prefix}_sector")

    return tb, tb_not_in_school, tb_sector
