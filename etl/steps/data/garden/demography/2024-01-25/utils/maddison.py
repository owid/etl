"""Format Maddison table accordingly.

Data is provided from -1800 to 1938.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

# Year boundaries
YEAR_MADDISON_START = 1800
YEAR_MADDISON_END = 1938
# Countries that do have data after 1938
countries_post_1938 = {
    "Argentina",
    "Bahamas",
    "Barbados",
    "Belize",
    "Bermuda",
    "Bolivia",
    "Brazil",
    "Canada",
    "Chile",
    "Colombia",
    "Costa Rica",
    "Cuba",
    "Dominican Republic",
    "Ecuador",
    "El Salvador",
    "Falkland Islands",
    "French Guiana",
    "Grenada",
    "Guadeloupe",
    "Guatemala",
    "Guyana",
    "Haiti",
    "Honduras",
    "Jamaica",
    "Leward Island (former)",
    "Martinique",
    "Mexico",
    "Netherlands Antilles",
    "Nicaragua",
    "Panama",
    "Paraguay",
    "Peru",
    "Puerto Rico",
    "Saint Barthelemy",
    "Saint Lucia",
    "Saint Vincent and the Grenadines",
    "Suriname",
    "Trinidad and Tobago",
    "Turks and Caicos Islands",
    "United States Virgin Islands",
    "Uruguay",
    "Venezuela",
}
# 1:M coutry equivalents
country_equivalents_1M = {
    "Kenya and Uganda (former)": ["Kenya", "Uganda"],  # OK
    "Czechoslovakia (former)": ["Czechia"],  # OK
    "French Indochina (former)": ["Cambodia", "Laos", "Vietnam"],  # OK
    "French West Africa (former)": [  # unsure for togo
        "Benin",
        "Burkina Faso",
        "Guinea",
        "Cote d'Ivoire",
        "Mali",
        "Mauritania",
        "Niger",
        "Senegal",
        # "Togo",
    ],
    "Korea (former)": ["North Korea", "South Korea"],  # OK
    "Rwanda and Burundi": ["Burundi", "Rwanda"],  # OK
    "Syria and Lebanon (former)": ["Lebanon", "Syria"],  # OK
    "Sudan (former)": ["South Sudan", "Sudan"],
    # "Russia/USSR (former)": [  # unclear about latvia, lithuania and estonia
    #     "Armenia",
    #     "Azerbaijan",
    #     "Belarus",
    #     "Estonia",
    #     "Georgia",
    #     "Kazakhstan",
    #     "Kyrgyzstan",
    #     "Latvia",
    #     "Lithuania",
    #     "Moldova",
    #     "Russia",
    #     "Tajikistan",
    #     "Turkmenistan",
    #     "Ukraine",
    #     "Uzbekistan",
    # ],
    # Unclear how serbia/yugoslavia, ottoman and ottoman balkans should be split
    # "Serbia/Yugoslavia (former)": [
    #     "Bosnia and Herzegovina",
    #     "Croatia",
    #     "Kosovo",
    #     "Montenegro",
    #     "North Macedonia",
    #     "Serbia",
    #     "Slovenia",
    # ],
    # "Ottoman Balkans (former)": [
    #     "Albania",
    #     "Bulgaria",
    #     "Greece",
    #     "Romania",
    # ],
    # "Ottoman Empire (former) / Turkey": [
    #     "Turkey",
    # ],
}
# M:1 coutry equivalents
country_equivalents_M1 = {
    "Yemen": ["North Yemen (former)", "South Yemen (former)"],
    "Malaysia and Singapore": ["British Malaya", "Sarawak (former)"],
}


def format_maddison(tb: Table, tb_reference: Table) -> Table:
    """Format Maddison table."""
    # Sanity checks IN: No data between 1938 and 1950, except for accepted countries.
    assert tb[
        (tb.loc[~tb["country"].isin(countries_post_1938), "year"] > YEAR_MADDISON_END) & (tb.year < 1950)
    ].empty, f"Unexpected years for Maddison. Should be between {YEAR_MADDISON_START} and {YEAR_MADDISON_END}!"

    # Rename columns, dtypes, sort rows
    columns_rename = {
        "country": "country",
        "year": "year",
        "population": "population",
    }
    tb = (
        tb.rename(columns=columns_rename, errors="raise")[columns_rename.values()]
        .assign(source="maddison")
        .astype(
            {
                "source": "str",
                "country": str,
                "population": "uint64",
                "year": "int64",
            }
        )
        .sort_values(["country", "year"])
        .reset_index(drop=True)
    )
    # Add population estimates from 1:M country equivalents.
    # tb = estimate_population_1_to_M(tb, tb_reference)

    # Add suffix to entity 'Netherlands Antilles'
    tb.loc[tb["country"] == "Netherlands Antilles", "country"] = "Netherlands Antilles (maddison)"
    return tb


def estimate_population_1_to_M(tb: Table, tb_reference: Table) -> Table:
    """Add population estimates from 1:M country equivalents."""
    for country_former, countries_new in country_equivalents_1M.items():
        population_former = tb_reference.loc[countries_new, "population"].sum()
        for country_new in countries_new:
            ratio = tb_reference.loc[country_new, "population"] / population_former
            tb_new = tb.loc[tb["country"] == country_former].copy()
            tb_new.loc[:, "country"] = country_new
            tb_new.loc[:, "population"] = tb_new["population"] * ratio
            tb = pr.concat([tb, tb_new])
    return tb
