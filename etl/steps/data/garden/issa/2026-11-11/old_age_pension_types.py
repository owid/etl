"""Load a meadow dataset and create a garden dataset."""

import re

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Year assigned to all countries.
# NOTE: The ISSA updates its information region by region. In this release, it's up to date as of July 2023 for most
#  of Europe, January 2024 for Asia and the Pacific and Eurasia, July 2024 for Africa, and July 2025 for the Americas.
#  We use the most recent of those years for all countries. Update it, and the reference dates in the metadata, at
#  each update.
YEAR = 2025

# Columns of the source table, one per type of program.
CONTRIBUTORY = ["contributory_flat_rate", "contributory_earnings_related"]
TAX_FUNDED = ["non_contributory_means_tested", "non_contributory_universal"]
SAVINGS = ["provident_fund", "mandatory_occupational_pension", "individual_accounts"]
PROGRAMS = CONTRIBUTORY + TAX_FUNDED + SAVINGS

# Start of each footnote's text on the source page, to check that their meaning hasn't changed.
# NOTE: If a check fails on an update, read the new text and update the labels in FOOTNOTE_LABELS too.
EXPECTED_FOOTNOTES = {
    "a": "The benefit formula contains a flat-rate component as well as an element based on earnings",
    "b": "A universal pension is paid regardless of earnings or contributions, but the amount may vary.",
    "c": "The government provides a guaranteed minimum pension.",
    "d": "The benefit is equal to total employer and employee contributions plus accrued interest, divided by",
    "e": "The benefit is equal to total employer and employee contributions plus accrued interest.",
    "f": "This programme is closed to new entrants and is being phased out.",
    "g": "Individuals may chose to affiliate in social insurance or the mandatory individual account.",
}

# Words added to "Yes" for each footnote, in the program indicators.
FOOTNOTE_LABELS = {
    "a": "with flat-rate and earnings-related parts",
    "b": "the amount can vary or be reduced at higher incomes",
    "c": "with a guaranteed minimum pension",
    "d": "based on notional accounts",
    "e": "the pension equals contributions plus interest",
    "f": "closed to new entrants",
}
# Footnote "g" (a choice between social insurance and individual accounts) reads differently in each column.
FOOTNOTE_G_LABELS = {
    "contributory_earnings_related": "workers can choose individual accounts instead",
    "individual_accounts": "workers can choose social insurance instead",
}

# Labels of the overall classifications.
MAIN_TYPE_LABELS = {
    "contributory": "Contributory pension",
    "tax_funded": "Tax-funded pension only",
    "savings": "Mandatory savings only",
}
MIX_LABELS = {
    (True, False, False): "Contributory pension only",
    (True, True, False): "Contributory and tax-funded pensions",
    (True, False, True): "Contributory pension and mandatory savings",
    (True, True, True): "Contributory and tax-funded pensions, and mandatory savings",
    (False, True, False): "Tax-funded pension only",
    (False, True, True): "Tax-funded pension and mandatory savings",
    (False, False, True): "Mandatory savings only",
}

# Pattern of a cell in the source table: empty, or "X" preceded by footnote letters (e.g. "X", "a X", "c, g X").
CELL_PATTERN = re.compile(r"^$|^(?:([a-g](?:, [a-g])*) )?X$")


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("old_age_pension_types")

    # Read tables from meadow dataset.
    tb = ds_meadow.read("old_age_pension_types")
    tb_footnotes = ds_meadow.read("footnotes")

    # Check the cells and footnotes before using them.
    sanity_check_inputs(tb=tb, tb_footnotes=tb_footnotes)

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Turn each cell into a label: "No", "Yes", or "Yes" with the details its footnotes give.
    tb_programs = tb[PROGRAMS].copy()
    for column in PROGRAMS:
        tb[column] = tb[column].astype(str).apply(lambda cell, column=column: label_cell(cell=cell, column=column))
        tb[column] = tb[column].copy_metadata(tb_programs[column])

    # Classify each country by the programs that are open to new workers (closed programs, footnote "f", are left out).
    is_open = {
        column: tb_programs[column].astype(str).str.contains("X") & ~tb_programs[column].astype(str).str.contains("f")
        for column in PROGRAMS
    }
    has_contributory = is_open[CONTRIBUTORY[0]] | is_open[CONTRIBUTORY[1]]
    has_tax_funded = is_open[TAX_FUNDED[0]] | is_open[TAX_FUNDED[1]]
    has_savings = is_open[SAVINGS[0]] | is_open[SAVINGS[1]] | is_open[SAVINGS[2]]

    # Type of public pension: contributory, tax-funded only, or none (mandatory savings only).
    tb["type_of_public_pension"] = tb["contributory_flat_rate"].copy()
    tb.loc[has_contributory, "type_of_public_pension"] = MAIN_TYPE_LABELS["contributory"]
    tb.loc[~has_contributory & has_tax_funded, "type_of_public_pension"] = MAIN_TYPE_LABELS["tax_funded"]
    tb.loc[~has_contributory & ~has_tax_funded & has_savings, "type_of_public_pension"] = MAIN_TYPE_LABELS["savings"]

    # Mix of contributory pensions, tax-funded pensions and mandatory savings.
    tb["pension_system_mix"] = tb["contributory_flat_rate"].copy()
    for (contributory, tax_funded, savings), label in MIX_LABELS.items():
        mask = (has_contributory == contributory) & (has_tax_funded == tax_funded) & (has_savings == savings)
        tb.loc[mask, "pension_system_mix"] = label

    # Add the year of the information.
    tb["year"] = YEAR

    # Improve table format.
    tb = tb.format(["country", "year"])

    # Check the output.
    sanity_check_outputs(tb=tb)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def label_cell(cell: str, column: str) -> str:
    """Turn a source cell into a label, e.g. "" -> "No", "X" -> "Yes", "f X" -> "Yes, closed to new entrants"."""
    if cell == "":
        return "No"
    match = CELL_PATTERN.match(cell)
    assert match is not None, f"Unexpected cell {cell!r} in {column}."
    letters = match.group(1).split(", ") if match.group(1) else []
    details = [FOOTNOTE_G_LABELS[column] if letter == "g" else FOOTNOTE_LABELS[letter] for letter in letters]
    return ", ".join(["Yes"] + details)


def sanity_check_inputs(tb: Table, tb_footnotes: Table) -> None:
    # The table has one row per country, and the columns we expect.
    assert set(tb.columns) == {"country"} | set(PROGRAMS), "The columns of the source table have changed."
    assert tb["country"].is_unique, "Duplicate countries in the source table."
    assert len(tb) >= 190, f"Fewer countries than expected in the source table: {len(tb)}."

    # Each cell is empty or an "X" with footnote letters.
    for column in PROGRAMS:
        bad = sorted(set(cell for cell in tb[column].astype(str) if not CELL_PATTERN.match(cell)))
        assert not bad, f"Unexpected cells in {column}: {bad}."
        # Footnote "g" only appears where we have a label for it.
        if column not in FOOTNOTE_G_LABELS:
            assert not tb[column].astype(str).str.contains("g").any(), f"Footnote g appears in {column}."

    # Every country has at least one program.
    has_program = (tb[PROGRAMS].astype(str) != "").any(axis=1)
    assert has_program.all(), f"Countries with no program: {sorted(tb.loc[~has_program, 'country'])}."

    # The footnotes still say what our labels assume.
    footnotes = dict(zip(tb_footnotes["key"].astype(str), tb_footnotes["text"].astype(str)))
    for letter, start in EXPECTED_FOOTNOTES.items():
        assert footnotes.get(letter, "").startswith(start), f"Footnote {letter} has changed: {footnotes.get(letter)!r}."
    letters = {key for key in footnotes if len(key) == 1}
    assert letters == set(EXPECTED_FOOTNOTES), f"Footnote letters have changed: {sorted(letters)}."


def sanity_check_outputs(tb: Table) -> None:
    # No missing values, and every country got a classification.
    assert not tb.isna().any().any(), "Missing values in the output."
    assert set(tb["type_of_public_pension"]) <= set(MAIN_TYPE_LABELS.values()), (
        "A country has no type of public pension."
    )
    assert set(tb["pension_system_mix"]) <= set(MIX_LABELS.values()), "A country has no pension system mix."
