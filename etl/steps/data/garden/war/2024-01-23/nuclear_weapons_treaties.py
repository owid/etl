"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

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
LABEL_AGREED = "Signed"
# Label for the status "Ratification", "Accession" and "Succession", used for countries that legally commit to a treaty.
# "Ratification" denotes a country that has ratified the treaty after signing it.
# "Accession" denotes a country that has ratified the treaty without having signed it first.
# "Succession" denotes a country that has inherited the status of a predecessor that has ratified the treaty.
# NOTE: A priori it may be possible that a country inherits the status "Signatory" from a predecessor, but later on I
# check that this is never the case.
LABEL_COMMITTED = "Approved"
# Label for the exceptional status "Withdrawal", which denotes a country that has withdrawn from the legal commitment.
LABEL_WITHDRAWN = "Withdrawn"
# Label for all countries-years that are not posterior to either an agreement or a commitment.
LABEL_NOT_SIGNED = "Not signed"

# List of known withdrawals of any treaty.
WITHDRAWALS = [{"treaty": "Comprehensive Nuclear-Test-Ban Treaty", "country": "Russia", "date": "2023-11-03"}]


def run_sanity_checks(tb: Table) -> None:
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


def prioritize_status(statuses):
    # Prioritize the status of a country.
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


def expand_data_to_all_countries_and_years(tb: Table, tb_members: Table) -> Table:
    tb = tb.copy()
    # Extract the maximum year from the publication date of the data.
    year_max = int(tb["status"].metadata.origins[0].date_published.split("-")[0])

    # List all countries (by selecting non-historical countries from our regions dataset).
    # NOTE: Some states that are not independent member states of the UN do participate in the treaties.
    # This is specifically the case of Cook Islands, Niue, Palestine, and Vatican.
    # So, we need to include all UN members, but also all countries currently in the treaties data.
    countries = sorted(set(tb_members[(tb_members["membership_status"] == "Member")]["country"]) | set(tb["country"]))

    # For each treaty, find the minimum year (which should be the year when it was signed for the first time)
    # and find all combinations of countries and years.
    # Then concatenate those combinations for all treaties.
    all_combinations = pr.concat(
        [
            Table(
                pd.MultiIndex.from_product(
                    [[treaty], countries, range(tb[tb["treaty"] == treaty]["year"].min(), year_max)],
                    names=["treaty", "country", "year"],
                ).to_frame(index=False)
            )
            for treaty in tb["treaty"].unique()
        ]
    )
    tb = tb.merge(all_combinations, on=["treaty", "country", "year"], how="right")

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("nuclear_weapons_treaties")
    tb = ds_meadow["nuclear_weapons_treaties"].reset_index()

    # Load dataset of UN members and read its main table.
    # NOTE: We do not load the regions dataset because regions like Greenland or French Guiana would appear as countries
    # that do not sign the treaties. In reality, they are not independent members of the UN, but they are part of other
    # countries that are members (in these cases, Denmark and France, respectively).
    # So we load the list of independent UN members, and countries like Greenland and French Guiana will simply appear
    # as having no data.
    ds_members = paths.load_dataset("un_members")
    tb_members = ds_members["un_members"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns, and ensure all columns are string (to avoid issues with categorical columns).
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise").astype(str)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Run sanity checks.
    run_sanity_checks(tb)

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

    # Add a row for each treaty and country-year (with an empty status).
    tb = expand_data_to_all_countries_and_years(tb=tb, tb_members=tb_members)

    # Forward fill the status of each country-year, and for the years before the first event in a country, fill with a
    # generic status.
    tb["status"] = tb.groupby(["treaty", "country"])["status"].ffill()
    tb["status"] = tb["status"].fillna(LABEL_NOT_SIGNED)

    # Create a table with the number of countries that have each status for each treaty and year.
    tb_counts = tb.groupby(["treaty", "year", "status"]).count().reset_index().rename(columns={"country": "countries"})

    # Transpose the main table to have a column for the status of each treaty.
    tb = tb.pivot(index=["country", "year"], columns="treaty", values="status", join_column_levels_with=" ")

    # Transpose the counts table to have a column for the number of countries of each treaty.
    tb_counts = tb_counts.pivot(
        index=["year", "status"], columns=["treaty"], values="countries", join_column_levels_with=" - "
    )

    # Rename counts table.
    tb_counts.metadata.short_name = "nuclear_weapons_treaties_country_counts"

    # Make columns snake-case, set an appropriate index and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()
    tb_counts = tb_counts.underscore().set_index(["year", "status"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_counts], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    ds_garden.save()
