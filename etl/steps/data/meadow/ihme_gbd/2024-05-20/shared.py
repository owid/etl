import pandas as pd
from owid.catalog import Table


def clean_data(tb: Table) -> Table:
    tb = tb.rename(
        columns={
            "location_name": "country",
            "location": "country",
            "val": "value",
            "measure_name": "measure",
            "sex_name": "sex",
            "age_name": "age",
            "cause_name": "cause",
            "metric_name": "metric",
        },
        errors="ignore",
    )
    tb = tb.drop(
        columns=["measure_id", "location_id", "sex_id", "age_id", "cause_id", "metric_id", "upper", "lower"],
        errors="ignore",
    )
    msk = (tb["measure"].isin(["Prevalence", "Incidence"])) & (tb["metric"] == "Percent")
    tb = tb[~msk]
    return tb


def fix_percent(tb: Table) -> Table:
    """
    IHME doesn't seem to be consistent with how it stores percentages.
    If the maximum percent value for any cause of death is less than or equal 1,
    it indicates all values are 100x too small and we need to multiply values by 100
    """
    if "Percent" in tb["metric"].unique():
        if max(tb["value"][tb["metric"] == "Percent"]) <= 1:
            subset_percent = tb["metric"] == "Percent"
            tb.loc[subset_percent, "value"] *= 100
            # tb["value"][(tb["metric"] == "Percent")] = tb["value"][(tb["metric"] == "Percent")] * 100
    return tb
