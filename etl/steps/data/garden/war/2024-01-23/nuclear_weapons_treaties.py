"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to read from the data, and how to rename them.
COLUMNS = {
    "treaty": "treaty",
    "state": "country",
    "date": "date",
    "action": "status",
}

# New label to use for the status of a country.
# NOTE: The following explanations are simplifications of the legal terms used in the treaties (and may be inaccurate).
# Label for the status "Signatory", which denotes a country that endorses the treaty but does not legally commit to it.
LABEL_AGREED = "Agreed"
# Label for the status "Ratification", "Accession" and "Succession", used for countries that legally commit to a treaty.
# "Ratification" denotes a country that has ratified the treaty after signing it.
# "Accession" denotes a country that has ratified the treaty without having signed it first.
# "Succession" denotes a country that has inherited the status of a predecessor that has ratified the treaty.
# NOTE: A priori it may be possible that a country inherits the status "Signatory" from a predecessor, but later on I
# check that this is never the case.
LABEL_COMMITTED = "Committed"
# Label for the exceptional status "Withdrawal", which denotes a country that has withdrawn from the legal commitment.
LABEL_WITHDRAWN = "Withdrawn"


# List of known withdrawals of any treaty.
WITHDRAWALS = [{"treaty": "comprehensive_test_ban", "country": "Russia", "date": "2023-11-03"}]


def prioritize_status(statuses):
    """Prioritize the status of a country."""
    if LABEL_WITHDRAWN in statuses:
        # I assume that a country does not withdraw from a treaty in the same year that it joins, and that it
        # doesn't rejoin the treaty in the same year.
        assert set(statuses) == {LABEL_WITHDRAWN}
        return LABEL_WITHDRAWN

    if LABEL_COMMITTED in statuses:
        # If a country commits, that should be the final status (unless there is a withdrawal).
        return LABEL_COMMITTED
    else:
        # If not withdrawn or committed, the only possible status should be agreed. Check that.
        assert set(statuses) == {LABEL_AGREED}
        return LABEL_AGREED


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("nuclear_weapons_treaties")
    tb = ds_meadow["nuclear_weapons_treaties"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns; and ensure all columns are string (to avoid issues with categorical columns).
    tb = tb[COLUMNS.keys()].rename(columns=COLUMNS, errors="raise").astype(str)

    # Harmonize country names (of states and depositaries).
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Sanity checks.
    for treaty in set(tb["treaty"]):
        _tb = tb[tb["treaty"] == treaty].reset_index(drop=True)
        # Check that countries that have the status "Succession" did not previously have only the status "Signatory".
        # In other words, check that a country never inherits the "Signatory" status from a predecessor.
        # If confirmed, we can safely treat "Succession" as the same status as "Accession" and "Ratification".
        # NOTE: A country may have the status "Signatory", then change to a legally binding status, and then change to
        # the status "Succession". This is the case of Serbia for the partial-test-ban treaty.
        # But what I am checking is that a country cannot have only the status "Signatory" inherited from a predecessor.
        countries_with_succession = set(_tb[_tb["status"] == "Succession"]["country"])
        assert set(_tb[_tb["country"].isin(countries_with_succession)]["status"]) != {"Signatory"}
        # By definition, a country with the status "Accession" is one that commits to the treaty without having a prior
        # status "Signatory".
        # Check that a country that has the status "Accession" does not have any prior status "Signatory".
        countries_with_accession = set(_tb[_tb["status"] == "Accession"]["country"])
        assert "Signatory" not in set(_tb[_tb["country"].isin(countries_with_accession)]["status"])
    error = "The list of withdrawals has changed."
    assert sorted(tb[tb["status"] == "Withdrawal"].drop(columns=["status"]).to_dict(orient="records")) == sorted(
        WITHDRAWALS  # type: ignore
    ), error

    # For simplicity, rename the status of a country to simpler terms.
    tb.loc[tb["status"] == "Signatory", "status"] = LABEL_AGREED
    tb.loc[tb["status"].isin(["Succession", "Accession", "Ratification"]), "status"] = LABEL_COMMITTED
    tb.loc[tb["status"] == "Withdrawal", "status"] = LABEL_WITHDRAWN

    # Add a column for the year of each event.
    tb["year"] = tb["date"].str[:4].astype(int)

    # For each year, keep only the latest status of each country.
    tb = tb.groupby(["treaty", "country", "year"], as_index=False).agg(
        {"status": lambda x: prioritize_status(x.values)}
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["treaty", "country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
