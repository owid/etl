"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEAR_MIN = 1790
YEAR_MAX = 1955

# Countries that are relevant for the analysis
COUNTRIES_RELEVANT = [
    "Afghanistan",
    "Albania",
    "Algeria",
    "Andorra",
    "Angola",
    "Armenia",
    "Azerbaijan",
    "Bahrain",
    "Bangladesh",
    "Belarus",
    "British Virgin Islands",
    "Burundi",
    "Cambodia",
    "Cameroon",
    "Central African Republic",
    "Chad",
    "Comoros",
    "South Korea",
    "North Korea",
    "Democratic Republic of Congo",
    "Djibouti",
    "Eritrea",
    "Falkland Islands",
    "Gabon",
    "Georgia",
    "Germany",
    "Guam",
    "Iceland",
    "India",
    "Iran",
    "Iraq",
    "Iran",
    "Ireland",
    "Japan",
    "Kazakhstan",
    "Kenya",
    "Kuwait",
    "Kyrgyzstan",
    "Laos",
    "Latvia",
    "Libya",
    "Liechtenstein",
    "Madagascar",
    "Malawi",
    "Mali",
    "Marshall Islands",
    "Mauritania",
    "Micronesia (country)",  # TODO
    "Namibia",
    "Nauru",
    "New Caledonia",
    "New Zealand",
    "Niger",
    "Nigeria",
    "Niue",
    # "Northern Mariana Islands",  # TODO
    # "Palestine",  # TODO
    "Oman",
    "Palau",
    "Paraquay",
    "Qatar",
    # "Russia",  # TODO
    "Rwanda",
    "Reunion",
    "Saint Helena",
    "Saint Kitts and Nevis",
    "Saint Lucia",
    "Saint Pierre and Miquelon",
    "Saint Vincent and the Grenadines",
    "Samoa",
    "San Marino",
    "Sao Tome and Principe",
    "Saudi Arabia",
    "Senegal",
    "Serbia",
    "Sierra Leone",
    "Slovakia",
    "Solomon Islands",
    "Somalia",
    "South Africa",
    "Sudan",
    "Tajikistan",
    "Thailand",
    "Tonga",
    "Turkmenistan",
    "Uganda",
    "Ukraine",
    "United Arab Emirates",
    "Uzbekistan",
    "Vanuatu",
    "Yemen",
    "Zambia",
]


def standardize_tb(tb, tb_main, col_population: str = "population"):
    tb = tb.loc[:, ["country", "year", "population"]]
    tb = tb.loc[tb["country"].isin(tb_main["country"].unique())]
    tb = tb.loc[(tb["year"] >= YEAR_MIN) & (tb["year"] <= YEAR_MAX)]
    tb["population"] = tb["population"].round().astype("Int64")

    tb = tb.rename(
        columns={
            "population": col_population,
        }
    )
    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("population_explore")
    ds_omm = paths.load_dataset("population", namespace="demography")
    ds_hyde = paths.load_dataset("baseline")
    ds_gm = paths.load_dataset("population", namespace="gapminder")
    ds_wpp = paths.load_dataset("un_wpp")

    # Read table from meadow dataset.
    tb = ds_meadow["population_explore"].reset_index()
    tb_omm = ds_omm["population"].reset_index()
    tb_hyde = ds_hyde["population"].reset_index()
    tb_gm = ds_gm["population"].reset_index()
    tb_wpp = ds_wpp["population"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Format OMM column
    tb_omm = standardize_tb(tb_omm, tb, "population_omm")
    # Format HYDE
    tb_hyde = standardize_tb(tb_hyde, tb, "population_hyde")
    # Format Gapminder
    tb_gm = standardize_tb(tb_gm, tb, "population_gm")
    # Format WPP
    tb_wpp = tb_wpp[
        (tb_wpp["age"] == "all")
        & (tb_wpp["sex"] == "all")
        & (tb_wpp["metric"] == "population")
        & (tb_wpp["variant"] == "estimates")
    ].rename(columns={"location": "country", "value": "population"})
    tb_wpp = standardize_tb(tb_wpp, tb, "population_wpp")

    # Merge
    tb = pr.multi_merge(
        tables=[tb, tb_omm, tb_hyde, tb_gm, tb_wpp],
        on=["country", "year"],
        how="outer",
    )
    tb["diff"] = tb["population_v2"] - tb["population_omm"]

    # Add cut versions
    tb["population_hyde_cut"] = tb.loc[tb["year"] <= 1800, "population_hyde"]
    tb["population_gm_cut"] = tb.loc[(tb["year"] >= 1801) & (tb["year"] <= 1950), "population_gm"]
    tb["population_wpp_cut"] = tb.loc[tb["year"] >= 1950, "population_wpp"]

    # Filter relevant countries
    # tb = tb.loc[tb["country"].isin(COUNTRIES_RELEVANT)]

    # Format
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        formats=["csv", "feather"],
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
