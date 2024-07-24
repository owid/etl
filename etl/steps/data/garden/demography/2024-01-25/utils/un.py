"""Format WPP table accordingly.

UN WPP provides estimates for the period 1950 - 2021 and projections for the period 2022 - 2100.

We only use projections with medium-fertility variant.
"""


from owid.catalog import Table

# Year boundaries
YEAR_WPP_START = 1950
YEAR_WPP_PROJECTIONS_START = 2022
YEAR_WPP_END = 2100


def format_un(tb: Table) -> Table:
    """Format UN WPP table."""
    # Only keep data for general population (all sexes, all ages, etc.)
    tb = tb.loc[
        (
            (tb["metric"] == "population")
            & (tb["sex"] == "all")
            & (tb["age"] == "all")
            & (tb["variant"].isin(["estimates", "medium"]))
        ),
        ["location", "year", "variant", "value"],
    ]

    # Sanity checks IN
    assert (
        tb.loc[tb["variant"] == "estimates", "year"].min() == YEAR_WPP_START
    ), f"Unexpected start year for WPP estimates. Should be {YEAR_WPP_START}!"
    assert (
        tb.loc[tb["variant"] == "estimates", "year"].max() == YEAR_WPP_PROJECTIONS_START - 1
    ), f"Unexpected end year for WPP estimates. Should be {YEAR_WPP_PROJECTIONS_START - 1}!"
    assert (
        tb.loc[tb["variant"] == "medium", "year"].min() == YEAR_WPP_PROJECTIONS_START
    ), f"Unexpected start year for WPP projections. Should be {YEAR_WPP_PROJECTIONS_START}!"
    assert (
        tb.loc[tb["variant"] == "medium", "year"].max() == YEAR_WPP_END
    ), f"Unexpected end year for WPP projections. Should be {YEAR_WPP_END}!"

    # Rename columns, sort rows
    columns_rename = {
        "location": "country",
        "year": "year",
        "value": "population",
    }
    tb = (
        tb.rename(columns=columns_rename, errors="raise")[columns_rename.values()]
        .assign(source="unwpp")
        .astype(
            {
                "source": "str",
                "country": str,
                "population": "uint64",
                "year": "uint64",
            }
        )
        .sort_values(["country", "year"])
        .reset_index(drop=True)
    )

    # Exclude countries
    countries_exclude = [
        "Northern America",
        "Latin America & Caribbean",
        "Land-locked developing countries (LLDC)",
        "Latin America and the Caribbean",
        "Least developed countries",
        "Less developed regions",
        "Less developed regions, excluding China",
        "Less developed regions, excluding least developed countries",
        "More developed regions",
        "Small island developing states (SIDS)",
        "High-income countries",
        "Low-income countries",
        "Lower-middle-income countries",
        "Upper-middle-income countries",
    ]
    tb = tb.loc[~tb.country.isin(countries_exclude)]

    # Sanity checks OUT
    assert tb.groupby(["country", "year"])["population"].count().max() == 1

    return tb
