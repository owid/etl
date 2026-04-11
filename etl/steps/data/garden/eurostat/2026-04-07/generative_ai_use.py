"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Indicator code for generative AI use in the last 3 months (overall, not filtered by purpose).
INDIC_IS = "I_IUAI"

# Unit: percentage of individuals (not as share of internet users).
UNIT = "PC_IND"

# Individual type codes and their corresponding output column names.
IND_TYPES = {
    "IND_TOTAL": "pct_used_genai_16_74",
    "Y16_24": "pct_used_genai_16_24",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its table.
    ds_meadow = paths.load_dataset("generative_ai_use")
    tb = ds_meadow.read("generative_ai_use")

    #
    # Process data.
    #
    # Filter to the relevant indicator and unit.
    tb = tb[(tb["indic_is"] == INDIC_IS) & (tb["unit"] == UNIT)].reset_index(drop=True)

    # Keep only the two age groups we need.
    tb = tb[tb["ind_type"].isin(IND_TYPES)].reset_index(drop=True)

    # Rename geo to country and value to a common column.
    tb = tb.rename(columns={"geo": "country", "value": "share"}, errors="raise")

    # Drop now-unnecessary dimension columns.
    tb = tb.drop(columns=["indic_is", "unit"], errors="raise")

    # Harmonize country names.
    tb = paths.regions.harmonize_names(
        tb=tb,
    )

    # Strip flags (letters) from values and convert to float.
    tb["share"] = tb["share"].astype(str).str.replace(r"[a-z\s:]", "", regex=True)
    tb["share"] = tb["share"].replace("", float("nan")).astype("Float64")

    # Drop rows without data.
    tb = tb.dropna(subset=["share"]).reset_index(drop=True)

    # Split by age group and rename the share column to preserve origins through rename (not pivot).
    tables = []
    for ind_type_code, col_name in IND_TYPES.items():
        t = tb[tb["ind_type"] == ind_type_code].drop(columns=["ind_type"]).reset_index(drop=True)
        t = t.rename(columns={"share": col_name}, errors="raise")
        tables.append(t)

    # Merge the two age-group tables.
    tb = tables[0].merge(tables[1], on=["country", "year"], how="outer")

    # Format the table.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
