"""Load a meadow dataset and create a garden dataset."""

from typing import Dict

import owid.catalog.processing as pr
import pandas as pd
import plotly.express as px
from owid.catalog import Table
from owid.datautils.dataframes import map_series

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Dataset codes to select, and their corresponding names.
DATASET_CODES_AND_NAMES = {
    ####################################################################################################################
    # Gas and electricity prices.
    # NOTE: Prices are given per semester.
    "nrg_pc_202": "Gas prices for household consumers",  # bi-annual data (from 2007)
    "nrg_pc_203": "Gas prices for non-household consumers",  # bi-annual data (from 2007)
    "nrg_pc_204": "Electricity prices for household consumers",  # bi-annual data (from 2007)
    "nrg_pc_205": "Electricity prices for non-household consumers",  # bi-annual data (from 2007)
    ####################################################################################################################
    # Gas and electricity prices components.
    "nrg_pc_202_c": "Gas prices components for household consumers",  # annual data (from 2007)
    "nrg_pc_203_c": "Gas prices components for non-household consumers",  # annual data (from 2007)
    "nrg_pc_204_c": "Electricity prices components for household consumers",  # annual data (from 2007)
    "nrg_pc_205_c": "Electricity prices components for non-household consumers",  # annual data (from 2007)
    ####################################################################################################################
    # Historical data.
    # NOTE: For now I think we will have to ignore historical data.
    # I doesn't have a band for total price. Instead, it has different consumption bands (defined by "consom").
    # This field is a bit problematic.
    # The same value, e.g. "4141050" has different definitions for electricity ("Households - Da (annual consumption: 600 kWh)") and for gas ("Households - D1 (annual consumption: 8.37 GJ)").
    # The fact that the same value is used for different things is inconvenient, but not the main problem.
    # The main problem is that we would need to figure out how to properly aggregate these values to get totals (meanwhile current data comes with totals).
    # Additionally, historical data is disaggregated in "domestic" and "industrial", whereas current data is split in "households" and "non-households".
    # "consom": {}
    # "nrg_pc_202_h": "Gas prices for domestic consumers",  # bi-annual data (until 2007)
    # "nrg_pc_203_h": "Gas prices for industrial consumers",  # bi-annual data (until 2007)
    # "nrg_pc_204_h": "Electricity prices for domestic consumers",  # bi-annual data (until 2007)
    # "nrg_pc_205_h": "Electricity prices for industrial consumers",  # bi-annual data (until 2007)
    # "nrg_pc_206_h": "Electricity marker prices",  # bi-annual data (until 2007)
    ####################################################################################################################
    # Share for transmission and distribution in the network cost for gas and electricity.
    # NOTE: Decide if we could use the following.
    # "nrg_pc_206": "Share for transmission and distribution in the network cost for gas and electricity", # annual data (from 2007)
    ####################################################################################################################
    # The following are consumption volumes of electricity by consumption bands.
    # It doesn't give the relative consumption of each semester. If I understand correctly, it gives the percentage consumption of each band in the total consumption of the year.
    # "nrg_pc_202_v": "Gas consumption volumes for households", # annual data (from 2007)
    # "nrg_pc_203_v": "Gas consumption volumes for non-households", # annual data (from 2007)
    # "nrg_pc_204_v": "Electricity consumption volumes for households", # annual data (from 2007)
    # "nrg_pc_205_v": "Electricity consumption volumes for non-households", # annual data (from 2007)
}

# Columns to keep and how to rename them.
COLUMNS = {
    "nrg_cons": "consumption_band",
    "unit": "energy_unit",
    "tax": "price_level",
    "currency": "currency",
    "geo": "country",
    "time": "year",
    "dataset_code": "dataset_code",
    "nrg_prc": "price_component",
    "value": "value",
}

# Mappings of indexes.
# The definitions are copied from (replace [DATASET_CODE] with the dataset code):
# https://ec.europa.eu/eurostat/databrowser/view/[DATASET_CODE]/default/table?lang=en&category=nrg.nrg_price.nrg_pc
INDEXES_MAPPING = {
    # Currencies.
    "currency": {
        "EUR": "Euro",
        # Purchasing Power Standard
        "PPS": "PPS",
        "NAC": "National currency",
        "NAT": "National (former) currency",
    },
    # Flags (found right next to the value, as a string).
    # NOTE: Flag definitions are right below the data table in that page.
    "flag": {
        "e": "estimated",
        # "c": "confidential",
        "d": "definition differs",
        "b": "break in time series",
        "p": "provisional",
        "u": "low reliability",
        # "cd": "confidential, definition differs",
        # NOTE: I couldn't find the meaning of the following flag.
        # It happens for "Electricity prices for non-household consumers" for Cyprus in 2024 (for MWH_GE150000), and all values are zero.
        "n": "unknown flag",
    },
    # Price levels.
    "price_level": {
        # All taxes and levies included
        "I_TAX": "All taxes and levies included",
        # Excluding VAT and other recoverable taxes and levies
        # NOTE: This value gives a baseline price for electricity before any additional costs imposed by taxes or fees are added. It represents the net price of electricity.
        "X_TAX": "Excluding taxes and levies",
        # Excluding value-added tax (VAT) and other recoverable taxes and levies
        "X_VAT": "Excluding VAT and other recoverable taxes and levies",
    },
    # Consumption bands.
    # NOTE: This is only relevant for non-historical data.
    "consumption_band": {
        # Consumption bands for "Gas prices for household consumers" and "Gas price components for household consumers":
        # Consumption of GJ - all bands
        "TOT_GJ": "All bands",
        # Consumption less than 20 GJ - band D1
        "GJ_LT20": "<20GJ",
        # Consumption from 20 GJ to 199 GJ - band D2
        "GJ20-199": "20-199GJ",
        # Consumption 200 GJ or over - band D3
        "GJ_GE200": ">=200GJ",
        ################################################################################################################
        # Consumption bands for "Gas prices components for non-household consumers" and "Gas prices components for non-household consumers":
        # 'TOT_GJ': "All bands", # Already defined above.
        # Consumption less than 1 000 GJ - band I1
        "GJ_LT1000": "<1000GJ",
        # Consumption from 1 000 GJ to 9 999 GJ -band I2
        "GJ1000-9999": "1000-9999GJ",
        # Consumption from 10 000 GJ to 99 999 GJ - band I3
        "GJ10000-99999": "10000-99999GJ",
        # Consumption from 100 000 GJ to 999 999 GJ - band I4
        "GJ100000-999999": "100000-999999GJ",
        # Consumption from 1 000 000 GJ to 3 999 999 GJ - band I5
        "GJ1000000-3999999": "1000000-3999999GJ",
        # Consumption 4 000 000 GJ or over - band I6
        "GJ_GE4000000": ">=4000000GJ",
        ################################################################################################################
        # Consumption bands for "Electricity prices for household consumers" and "Electricity prices components for household consumers":
        # Consumption of kWh - all bands
        "TOT_KWH": "All bands",
        # Consumption less than 1 000 kWh - band DA
        "KWH_LT1000": "<1000kWh",
        # Consumption from 1 000 kWh to 2 499 kWh - band DB
        "KWH1000-2499": "1000-2499kWh",
        # Consumption from 2 500 kWh to 4 999 kWh - band DC
        "KWH2500-4999": "2500-4999kWh",
        # Consumption from 5 000 kWh to 14 999 kWh - band DD
        "KWH5000-14999": "5000-14999kWh",
        # Consumption for 15 000 kWh or over - band DE
        "KWH_GE15000": ">=15000kWh",
        # NOTE: In the electricity components dataset, there is an additional band, which contains *LE* but in the metadata it seems to correspond to greater or equal, band DE, so it must be a typo in the band name.
        # Consumption 15 000 kWh or over - band DE
        "KWH_LE15000": ">=15000kWh",
        ################################################################################################################
        # Consumption bands for "Electricity prices components for non-household consumers" and "Electricity prices components for non-household consumers":
        # Consumption of kWh - all bands
        # "TOT_KWH": "All bands",  # Already defined above.
        # Consumption less than 20 MWh - band IA
        "MWH_LT20": "<20MWh",
        # Consumption from 20 MWh to 499 MWh - band IB
        "MWH20-499": "20-499MWh",
        # Consumption from 500 MWh to 1 999 MWh - band IC
        "MWH500-1999": "500-1999MWh",
        # Consumption from 2 000 MWh to 19 999 MWh - band ID
        "MWH2000-19999": "2000-19999MWh",
        # Consumption from 20 000 MWh to 69 999 MWh - band IE
        "MWH20000-69999": "20000-69999MWh",
        # Consumption from 70 000 MWh to 149 999 MWh - band IF
        "MWH70000-149999": "70000-149999MWh",
        # Consumption 150 000 MWh or over - band IG
        "MWH_GE150000": ">=150000MWh",
        # NOTE: In the electricity components dataset, there is an additional band:
        # Consumption 149 999 MWh or less - bandS IA-IF
        "MWH_LE149999": "<=149999MWh",
        ####################################################################################################################
    },
    # Energy price components.
    "price_component": {
        # Gas prices components for household and non-household consumers
        # Energy and supply
        "NRG_SUP": "Energy and supply",
        # Network costs
        "NETC": "Network costs",
        # Taxes, fees, levies and charges
        "TAX_FEE_LEV_CHRG": "Taxes, fees, levies, and charges",
        # Value added tax (VAT)
        "VAT": "Value added tax (VAT)",
        # Renewable taxes
        "TAX_RNW": "Renewable taxes",
        # Capacity taxes
        "TAX_CAP": "Capacity taxes",
        # Environmental taxes
        "TAX_ENV": "Environmental taxes",
        # Renewable taxes allowance
        "TAX_RNW_ALLOW": "Renewable taxes allowance",
        # Capacity taxes allowances
        "TAX_CAP_ALLOW": "Capacity taxes allowances",
        # Environmental taxes allowance
        "TAX_ENV_ALLOW": "Environmental taxes allowance",
        # Other allowance
        "ALLOW_OTH": "Other allowance",
        # Other
        "OTH": "Other",
        # Electricity prices components for household and non-household consumers
        # All the above, plus the additional:
        # Nuclear taxes
        "TAX_NUC": "Nuclear taxes",
        # Nuclear taxes allowance
        "TAX_NUC_ALLOW": "Nuclear taxes allowance",
        # Taxes, fees, levies and charges allowance
        "TAX_FEE_LEV_CHRG_ALLOW": "Taxes, fees, levies, and charges allowance",
        # From the metadata page (https://ec.europa.eu/eurostat/cache/metadata/en/nrg_pc_204_sims.htm), these are the components:
        # * Energy and supply: generation, aggregation, balancing energy, supplied energy costs, customer services, after-sales management and other supply costs.
        # * Network cost: transmission and distribution tariffs, transmission and distribution losses, network costs, after-sale service costs, system service costs, and meter rental and metering costs.
        # * Value added taxes (VAT): as defined in Council Directive 2006/112/EC.
        # * Renewable taxes: taxes, fees, levies or charges relating to the promotion of renewable energy sources, energy efficiency and CHP generation.
        # * Capacity taxes: Taxes, fees, levies or charges relating to capacity payments, energy security and generation adequacy; taxes on coal industry restructuring; taxes on electricity distribution; stranded costs and levies on financing energy regulatory authorities or market and system operators.
        # * Environmental taxes: taxes, fees, levies or charges relating to air quality and for other environmental purposes; taxes on emissions of CO2 or other greenhouse gases. This component includes the excise duties.
        # * Nuclear taxes: taxes, fees, levies or charges relating to the nuclear sector, including nuclear decommissioning, inspections and fees for nuclear installations.
        # * All other taxes: taxes, fees, levies or charges not covered by any of the previous five categories: support for district heating; local or regional fiscal charges; island compensation; concession fees relating to licences and fees for the occupation of land and public or private property by networks or other devices.
    },
    # Energy units.
    "energy_unit": {
        # Gigajoule (gross calorific value - GCV)
        "GJ_GCV": "GJ",
        # Kilowatt-hour
        "KWH": "kWh",
        # The following is used in consumption volumes datasets.
        # "PC": "Percentage",
    },
}

# Dataset codes for prices and components.
DATASET_CODES_PRICES = ["nrg_pc_202", "nrg_pc_203", "nrg_pc_204", "nrg_pc_205"]
DATASET_CODES_COMPONENTS = ["nrg_pc_202_c", "nrg_pc_203_c", "nrg_pc_204_c", "nrg_pc_205_c"]
DATASET_CODE_TO_ENERGY_SOURCE = {
    "nrg_pc_202": "Gas",
    "nrg_pc_203": "Gas",
    "nrg_pc_204": "Electricity",
    "nrg_pc_205": "Electricity",
    "nrg_pc_202_c": "Gas",
    "nrg_pc_203_c": "Gas",
    "nrg_pc_204_c": "Electricity",
    "nrg_pc_205_c": "Electricity",
}
DATASET_CODE_TO_CONSUMER_TYPE_MAPPING = {
    "nrg_pc_202": "Household",
    "nrg_pc_203": "Non-household",
    "nrg_pc_204": "Household",
    "nrg_pc_205": "Non-household",
    "nrg_pc_202_c": "Household",
    "nrg_pc_203_c": "Non-household",
    "nrg_pc_204_c": "Household",
    "nrg_pc_205_c": "Non-household",
}


# The following components need to be present in the prices components datasets of a country-year-dataset-currency, otherwise its data will not be included.
MANDATORY_PRICE_COMPONENTS = [
    "Energy and supply",
    "Network costs",
    "Taxes, fees, levies, and charges",
]

# List of components that add up to the total price.
# NOTE: See find_best_combination_of_components to understand how this choice was made.
COMPONENTS_THAT_ADD_UP_TO_TOTAL = ["Energy and supply", "Network costs", "Taxes, fees, levies, and charges"]

# Label to use for the calculated total price based on the sum of the main components.
COMPONENTS_TOTAL_PRICE_LABEL = "Total price, including taxes"


def sanity_check_inputs(tb: Table) -> None:
    # Ensure all relevant dataset codes are present.
    error = "Some dataset codes are missing."
    assert set(DATASET_CODES_AND_NAMES) <= set(tb["dataset_code"]), error
    # Check that each dataset has only one value in fields "freq", "product", and "nrg_cons".
    # error = "Some datasets have more than one value in field 'freq'."
    # assert (tb.groupby("dataset_code")["freq"].nunique() == 1).all(), error
    # error = "Expected 'freq' column to be either A (annual) or S (bi-annual)."
    # assert set(tb["freq"].dropna()) == set(["A", "S"]), error
    # error = "Some datasets have more than one value in field 'product'."
    # assert (tb.dropna(subset="product").groupby("dataset_code")["product"].nunique() == 1).all(), error
    # error = "Expected 'product' column to be either 4100 (gas) or 6000 (electricity)."
    # assert set(tb["product"].dropna()) == set([4100, 6000]), error
    error = "Expected electricity prices to be measured in kWh."
    assert set(
        tb[tb["dataset_code"].isin(["nrg_pc_204", "nrg_pc_205", "nrg_pc_204_h", "nrg_pc_205_h", "nrg_pc_206_h"])][
            "energy_unit"
        ]
    ) == set(["KWH"]), error
    # error = "Expected 'customer' column to be empty, for the selected datasets."
    # assert set(tb["customer"].dropna()) == set(), error
    # error = "Expected 'consom' column to be empty, for the selected datasets (that column is only relevant for historical data)."
    # assert set(tb["consom"].dropna()) == set(), error
    for field, mapping in INDEXES_MAPPING.items():
        if field == "flag":
            # Flags need to first be extracted from the value (so they will be sanity checked later).
            continue
        error = f"Unexpected values in field '{field}'."
        assert set(tb[field].dropna()) == set(mapping), error


def prepare_inputs(tb: Table) -> Table:
    # Values sometimes include a letter, which is a flag. Extract those letters and create a separate column with them.
    # Note that sometimes there can be multiple letters (which means multiple flags).
    tb["flag"] = tb["value"].astype("string").str.extract(r"([a-z]+)", expand=False)
    tb["value"] = tb["value"].str.replace(r"[a-z]", "", regex=True)

    # Some values are start with ':' (namely ':', ': ', ': c', ': u', ': cd'). Replace them with nan.
    tb.loc[tb["value"].str.startswith(":"), "value"] = None

    # Assign a proper type to the column of values.
    tb["value"] = tb["value"].astype(float)

    # Create a clean column of years, and another of dates.
    tb["year-semester"] = tb["year"].str.strip().copy()
    tb["year"] = tb["year-semester"].str[0:4].astype(int)
    # For the date column:
    # * For the first semester, use April 1st.
    # * For the second semester, use October 1st.
    # * For annual data, use July 1st.
    semester_1_mask = tb["year-semester"].str.contains("S1")
    semester_2_mask = tb["year-semester"].str.contains("S2")
    annual_mask = tb["year-semester"].str.isdigit()
    error = "Unexpected values in field 'year-semester'."
    assert (semester_1_mask | semester_2_mask | annual_mask).all(), error
    tb["date"] = pd.to_datetime(tb["year"].astype(str) + "-07-01")
    tb.loc[semester_1_mask, "date"] = pd.to_datetime(tb[semester_1_mask]["year"].astype(str) + "-04-01")
    tb.loc[semester_2_mask, "date"] = pd.to_datetime(tb[semester_2_mask]["year"].astype(str) + "-10-01")

    return tb


def harmonize_indexes_and_countries(tb: Table) -> Table:
    # Add a column with the dataset name.
    tb["dataset_name"] = map_series(
        tb["dataset_code"],
        mapping=DATASET_CODES_AND_NAMES,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
        show_full_warning=True,
    )

    # Harmonize all other index names.
    for field, mapping in INDEXES_MAPPING.items():
        # Avoid categorical dtypes.
        tb[field] = tb[field].astype("string")
        not_null_mask = tb[field].notnull()
        tb.loc[not_null_mask, field] = map_series(
            tb[not_null_mask][field],
            mapping=mapping,
            warn_on_missing_mappings=True,
            warn_on_unused_mappings=True,
            show_full_warning=True,
        )

    # Harmonize country names.
    # Countries are given in NUTS (Nomenclature of Territorial Units for Statistics) codes.
    # Region codes are defined in: https://ec.europa.eu/eurostat/web/nuts/correspondence-tables
    # There are additional codes not included there, namely:
    # EA: Countries in the Euro Area, that use the Euro as their official currency.
    # In the historical datasets, there are some additional regions:
    # EU15: The 15 countries that made up the EU prior to its 2004 expansion.
    # EU25: The 25 member states after the 2004 enlargement, which added ten countries.
    # EU27_2007: The 27 EU member states in 2007.
    # EU27_2020: The 27 EU members after the United Kingdom left in 2020.
    # UA: Ukraine (not a member of the EU, but often included in some European data).
    # UK: United Kingdom (not a member since 2020, but included in some European data).
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    return tb


########################################################################################################################

# The code in this block is not used in the data pipeline, but it was useful to understand the data and justify some of the choices.


def compare_components_and_prices_data(tb: Table) -> None:
    # Compare biannual prices data (without averaging overs semesters) with annual components data.
    price_level = "All taxes and levies included"
    tb_biannual = tb[(tb["year-semester"].str.contains("S")) & (tb["currency"] == "Euro")].reset_index(drop=True)
    tb_biannual = tb_biannual[(tb_biannual["price_component_or_level"] == price_level)][
        ["dataset_code", "country", "date", "value"]
    ]
    # Similarly, for annual data, assign July 1st.
    tb_annual = tb[(~tb["year-semester"].str.contains("S")) & (tb["currency"] == "Euro")].reset_index(drop=True)
    tb_annual["dataset_code"] = tb_annual["dataset_code"].str.replace("_c", "")

    combination = COMPONENTS_THAT_ADD_UP_TO_TOTAL
    annual_components = (
        tb_annual[(tb_annual["price_component_or_level"].isin(combination))]
        .groupby(
            ["dataset_code", "country", "date"],
            observed=True,
            as_index=False,
        )
        .agg({"value": lambda x: x.sum(min_count=1)})
        .dropna()
        .reset_index(drop=True)
    )

    # Combine both datasets for plotting.
    compared = pd.concat(
        [annual_components.assign(**{"source": "components"}), tb_biannual.assign(**{"source": "prices"})],
        ignore_index=True,
    )
    # Only a few country-years could be compared this way. Most of the points in the prices datasets were missing.
    for dataset_code in compared["dataset_code"].unique():
        for country in sorted(set(compared["country"])):
            _compared = compared[(compared["dataset_code"] == dataset_code) & (compared["country"] == country)]
            if len(set(_compared["source"])) < 2:
                continue
            px.line(
                _compared,
                x="date",
                y="value",
                color="source",
                markers=True,
                title=f"{dataset_code} - {country}",
            ).update_yaxes(range=[0, None]).show()


def find_best_combination_of_components(tb: Table) -> None:
    # Check that the resulting total price for the components dataset (summing up components) is similar to the biannual electricity prices data.

    # Ideally, the prices obtained by adding up components (in the components dataset) should be similar to those obtained in the prices dataset.
    # However, both are very sparse (especially the prices dataset), and the prices dataset is also given in semesters, which makes it difficult to compare (without having the actual consumption of each semester to be able to compute a weighted average).
    # Transforming biannual data into annual data is not straightforward.
    # I tried simply taking the average, but what I found is that the annual components prices (summed over all components) tends to be systematically higher than the biannual prices (averaged over the two semester of the year). I suppose this was caused by doing a simple average instead of weighting by consumption. In semesters with higher consumption (e.g. winter), the increased demand tends to drive prices up. Annual prices, as far as I understand, are consumption-weighted averages, and therefore assign a larger weight to those semesters with higher prices. So, intuitively, it makes sense that the true annual prices tend to be higher than the averaged biannual prices.
    # We could create a weighted average, but we would need the actual consumption of each semester (which I haven't found straightaway).

    # Compute an annual average only if there is data for the two semesters.
    price_level = "All taxes and levies included"
    tb_biannual = tb[(tb["year-semester"].str.contains("S")) & (tb["currency"] == "Euro")].reset_index(drop=True)
    tb_biannual_filtered = (
        tb_biannual.dropna(subset="value")
        .groupby(
            ["country", "year", "dataset_code", "price_component_or_level"],
            observed=True,
            as_index=False,
        )
        .filter(lambda x: len(x) == 2)
    )
    tb_biannual = tb_biannual_filtered.groupby(
        ["country", "year", "dataset_code", "price_component_or_level"],
        observed=True,
        as_index=False,
    ).agg({"value": "mean"})
    tb_biannual = tb_biannual[(tb_biannual["price_component_or_level"] == price_level)][
        ["dataset_code", "country", "year", "value"]
    ]
    # Similarly, for annual data, assign July 1st.
    tb_annual = tb[(~tb["year-semester"].str.contains("S")) & (tb["currency"] == "Euro")].reset_index(drop=True)
    tb_annual["dataset_code"] = tb_annual["dataset_code"].str.replace("_c", "")

    def _get_annual_sum(tb_annual, combination):
        annual_components = (
            tb_annual[(tb_annual["price_component_or_level"].isin(combination))]
            .groupby(
                ["dataset_code", "country", "year"],
                observed=True,
                as_index=False,
            )
            .agg({"value": lambda x: x.sum(min_count=1)})
            .dropna()
            .reset_index(drop=True)
        )

        return annual_components

    import itertools

    from tqdm.auto import tqdm

    # Get all possible combinations of components.
    elements = INDEXES_MAPPING["price_component"].values()
    combinations = []
    for r in range(1, len(elements) + 1):
        combinations.extend(itertools.combinations(elements, r))
    # Keep only combinations that include "Energy and supply" and "Network costs".
    combinations = [c for c in combinations if "Energy and supply" in c and "Network costs" in c]

    # Brute-force analysis: Check which combination of components minimizes the error between the sum of components and the prices data.
    # NOTE: This takes about 4 minutes.
    error_median = []
    error_mean = []
    error_max = []
    for combination in tqdm(combinations):
        annual_components = _get_annual_sum(tb_annual, combination)
        compared = (
            tb_biannual.merge(
                annual_components,
                on=["dataset_code", "country", "year"],
                how="inner",
                suffixes=("_prices", "_components"),
            )
            .dropna()
            .reset_index(drop=True)
        )
        compared["pct"] = 100 * abs(compared["value_prices"] - compared["value_components"]) / compared["value_prices"]
        error_mean.append(compared["pct"].mean())
        error_median.append(compared["pct"].median())
        error_max.append(compared["pct"].max())
    # Find the combination that minimizes the error.
    results_df = pd.DataFrame(
        {
            "combination": combinations,
            "error_mean": error_mean,
            "error_median": error_median,
            "error_max": error_max,
        }
    )
    # There is no single combination that minimizes all error.
    set(results_df[results_df["error_mean"] == results_df["error_mean"].min()]["combination"])
    # After inspection, there are different combinations that minimize error (since some components are actually always zero). In terms of the minimum mean error, the combinations are:
    # ('Energy and supply', 'Network costs', 'Taxes, fees, levies, and charges'),
    # ('Energy and supply', 'Network costs', 'Taxes, fees, levies, and charges', 'Capacity taxes allowances'),
    # ('Energy and supply', 'Network costs', 'Taxes, fees, levies, and charges', 'Capacity taxes allowances', 'Nuclear taxes allowance'),
    # ('Energy and supply', 'Network costs', 'Taxes, fees, levies, and charges', 'Nuclear taxes allowance')
    # Given that some of those allowance components are actually (almost) always zero, it seems clear that
    # the best combination is, as expected:
    components_optimal = ["Energy and supply", "Network costs", "Taxes, fees, levies, and charges"]
    annual_components = _get_annual_sum(tb_annual, combination=components_optimal)
    compared = (
        tb_biannual.merge(
            annual_components, on=["dataset_code", "country", "year"], how="inner", suffixes=("_prices", "_components")
        )
        .dropna()
        .reset_index(drop=True)
    )
    compared["pct"] = 100 * abs(compared["value_prices"] - compared["value_components"]) / compared["value_prices"]
    compared.sort_values("pct", ascending=False).head(60)
    # For most countries, the agreement is good, but some country-years, the discrepancy is significant, e.g. Denmark non-household electricity in 2022 and 2023, with an error of 26%.
    # There are only a few other discrepancies above 10%.

    # Visually inspect these discrepancies.
    compared = pd.concat(
        [annual_components.assign(**{"source": "components"}), tb_biannual.assign(**{"source": "prices"})],
        ignore_index=True,
    )
    # Only a few country-years could be compared this way. Most of the points in the prices datasets were missing.
    for dataset_code in compared["dataset_code"].unique():
        for country in sorted(set(compared["country"])):
            if (
                len(
                    set(
                        compared[(compared["dataset_code"] == dataset_code) & (compared["country"] == country)][
                            "source"
                        ]
                    )
                )
                < 2
            ):
                continue
            px.line(
                compared[(compared["dataset_code"] == dataset_code) & (compared["country"] == country)],
                x="year",
                y="value",
                color="source",
                markers=True,
                title=f"{dataset_code} - {country}",
            ).update_yaxes(range=[0, None]).show()

    # Conclusions:
    # * The prices and components datasets coincide reasonably well. To recover prices, it seems that the components to be added up are just "Energy and supply", "Network costs", and "Taxes, fees, levies, and charges". For most countries, this combination gives a good agreement with the prices dataset. However, for some countries, there is a significant discrepancy.
    # * Numerically, I have checked that for all price components datasets, "Taxes, fees, levies and charges" coincides with the sum of 'Capacity taxes', 'Environmental taxes', 'Nuclear taxes', 'Renewable taxes', 'Value added tax (VAT)', 'Other'. For some country-years, there is a small discrepancy.
    # * What's not so clear is what happens with the "allowances". Is "Taxes, fees, levies, and charges allowance" the sum of all other "* allowance"? It's hard to know, since it's non-zero only once (nrg_pc_204_c Netherlands 2023). At that point, it does coincide with the sum of all other "* allowance". But there are other instances of non-zero "* allowance" where "Taxes...allowance" is not defined. It may be possible that allowances are not included in the prices dataset.
    # NOTE:
    # After communicating with Eurostat, they confirmed that the total price should indeed be the result of adding up "Energy and supply", "Network costs", and "Taxes, fees, levies, and charges". The allowances should not be added, they are already incorporated in the price including taxes.


def plot_final_comparison_between_prices_and_components_data(tb: Table) -> None:
    for country in tb["country"].unique():
        for consumer_type in tb["consumer_type"].unique():
            for source in tb["source"].unique():
                _tb = tb[
                    (tb["country"] == country)
                    & (tb["source"] == source)
                    & (tb["consumer_type"] == consumer_type)
                    & (
                        tb["price_component_or_level"].isin(
                            [COMPONENTS_TOTAL_PRICE_LABEL, "All taxes and levies included"]
                        )
                    )
                ]
                if len(_tb["price_component_or_level"].unique()) < 2:
                    continue
                px.line(
                    _tb,
                    x="date",
                    y="price_euro",
                    color="price_component_or_level",
                    markers=True,
                    title=f"{consumer_type} {source} - {country}",
                ).show()


########################################################################################################################


def select_and_prepare_relevant_data(tb: Table) -> Table:
    # All datasets have a energy unit except electricity components (both for household and non-households).
    # I assume the energy unit is kWh.
    error = "Expected electricity components (both for household and non-households) to have no energy unit. Remove this code."
    assert tb[tb["dataset_code"].isin(["nrg_pc_204_c", "nrg_pc_205_c"])]["energy_unit"].isnull().all(), error
    tb.loc[tb["dataset_code"].isin(["nrg_pc_204_c", "nrg_pc_205_c"]), "energy_unit"] = "kWh"

    error = "Expected all datasets to have the same energy unit (kWh)."
    assert (
        tb.groupby(["dataset_code"], observed=True, as_index=False)
        .agg({"energy_unit": lambda x: "kWh" in x.unique()})["energy_unit"]
        .all()
    ), error
    # Select the same energy unit for all datasets (kWh).
    tb = tb[tb["energy_unit"] == "kWh"].drop(columns=["energy_unit"], errors="raise").reset_index(drop=True)

    # Convert prices from price per kWh to price per MWh.
    tb["value"] *= 1000

    # For convenience, instead of having a column for price component (for components datasets) and price level (for prices datasets), create a single column with the price component or level.
    assert tb[(tb["price_level"].isnull()) & (tb["price_component"].isnull())].empty
    assert tb[(tb["price_level"].notnull()) & (tb["price_component"].notnull())].empty
    tb["price_component_or_level"] = tb["price_level"].fillna(tb["price_component"])
    tb = tb.drop(columns=["price_level", "price_component"], errors="raise")

    # After inspection, it looks like the "All bands" consumption is very sparse in the prices datasets.
    # One option (if we decided to use the prices dataset) would be to use the more common consumption bands only, which are better informed.
    # In the components dataset, "All bands" seems to be less sparse (at least from 2019 onwards).
    # To get the total price from the components dataset, we would need to add up components.
    # But we would need to figure out which one is the subset of components that ensures no double-counting.
    tb = (
        tb[tb["consumption_band"] == "All bands"]
        .drop(columns=["consumption_band"], errors="raise")
        .reset_index(drop=True)
    )

    # Find the combination of price components that needs to be summed up to recover the full prices.
    # NOTE: Uncomment to perform the analysis again, and see conclusions in the following function to understand the choices.
    # find_best_combination_of_components(tb=tb)

    # Visually compare the resulting prices obtained by adding up certain components, with the original prices data.
    # NOTE: Uncomment to perform some visual checks.
    # compare_components_and_prices_data(tb=tb)

    # Remove groups (of country-year-dataset-currency) from the components dataset for which certain components (e.g. "Energy and supply") are not included.
    # For example, Albania doesn't have "Energy and supply" costs for household electricity, but it does have other components (e.g. "Network costs").
    tb.loc[
        (tb["dataset_code"].isin(DATASET_CODES_COMPONENTS))
        & (
            ~tb.groupby(["country", "year", "currency"])["price_component_or_level"].transform(
                lambda x: all(comp in x.tolist() for comp in MANDATORY_PRICE_COMPONENTS)
            )
        ),
        "value",
    ] = None

    # Remove empty rows.
    tb = tb.dropna(subset=["value"]).reset_index(drop=True)

    # Remove data with certain flags.
    tb = tb[
        ~tb["flag"].isin(
            [
                "confidential",
                "definition differs",
                "low reliability",
                "confidential, definition differs",
                "unknown flag",
            ]
        )
    ].reset_index(drop=True)
    error = "Unexpected flag values."
    assert set(tb["flag"].dropna()) <= set(["estimated", "break in time series", "provisional", "unknown flag"]), error

    # Add total price to the components dataset, by adding up the contribution of the main components.
    tb_components_total = (
        tb[
            tb["dataset_code"].isin(DATASET_CODES_COMPONENTS)
            & (tb["price_component_or_level"].isin(COMPONENTS_THAT_ADD_UP_TO_TOTAL))
        ]
        .groupby(
            ["currency", "country", "year", "dataset_code", "year-semester", "date", "dataset_name"],
            observed=True,
            as_index=False,
        )
        .agg({"value": "sum"})
        .assign(**{"price_component_or_level": COMPONENTS_TOTAL_PRICE_LABEL})
    )
    tb = pr.concat([tb, tb_components_total], ignore_index=True)

    # Create a column for the energy source.
    tb["source"] = map_series(
        tb["dataset_code"],
        mapping=DATASET_CODE_TO_ENERGY_SOURCE,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
        show_full_warning=True,
    )
    error = "Unexpected energy source."
    assert set(tb["source"]) == set(["Gas", "Electricity"]), error

    # Create a column for the consumer type.
    tb["consumer_type"] = map_series(
        tb["dataset_code"],
        mapping=DATASET_CODE_TO_CONSUMER_TYPE_MAPPING,
        warn_on_missing_mappings=True,
        warn_on_unused_mappings=True,
        show_full_warning=True,
    )
    error = "Unexpected consumer type."
    assert set(tb["consumer_type"]) == set(["Household", "Non-household"]), error

    # Drop unnecessary columns.
    tb = tb.drop(columns=["flag", "year-semester", "dataset_name"], errors="raise")

    # It would be confusing to keep different national currencies, so, keep only Euro and PPS.
    tb = tb[tb["currency"].isin(["Euro", "PPS"])].reset_index(drop=True)

    # Separate euros and PPS in two different columns.
    tb = (
        tb[tb["currency"] == "Euro"]
        .drop(columns=["currency"])
        .merge(
            tb[tb["currency"] == "PPS"].drop(columns=["currency"]),
            how="outer",
            on=["country", "year", "date", "dataset_code", "source", "price_component_or_level", "consumer_type"],
            suffixes=("_euro", "_pps"),
        )
        .rename(columns={"value_euro": "price_euro", "value_pps": "price_pps"}, errors="raise")
    )

    return tb


def prepare_wide_tables(tb: Table) -> Dict[str, Table]:
    # Table for average prices (in euros) of gas and electricity prices of household and non-household consumers.
    tb_prices_euro = tb[tb["dataset_code"].isin(DATASET_CODES_PRICES)].pivot(
        index=["country", "date"],
        columns=["source", "consumer_type", "price_component_or_level"],
        values="price_euro",
        join_column_levels_with="-",
    )
    # Table for average prices (in PPS) of gas and electricity prices of household and non-household consumers.
    tb_prices_pps = tb[tb["dataset_code"].isin(DATASET_CODES_PRICES)].pivot(
        index=["country", "date"],
        columns=["source", "consumer_type", "price_component_or_level"],
        values="price_pps",
        join_column_levels_with="-",
    )
    # Improve tables format.
    tb_prices_euro = tb_prices_euro.format(["country", "date"], short_name="gas_and_electricity_prices_euro_flat")
    tb_prices_pps = tb_prices_pps.format(["country", "date"], short_name="gas_and_electricity_prices_pps_flat")

    # Improve column names.
    tb_prices_euro = tb_prices_euro.rename(
        columns={column: column.replace("__", "_") + "_euro" for column in tb_prices_euro.columns}, errors="raise"
    )
    tb_prices_pps = tb_prices_pps.rename(
        columns={column: column.replace("__", "_") + "_pps" for column in tb_prices_pps.columns}, errors="raise"
    )

    # Table for price components (in euros) of gas and electricity prices of household and non-household consumers.
    tb_price_components_euro = tb[tb["dataset_code"].isin(DATASET_CODES_COMPONENTS)].pivot(
        index=["country", "year"],
        columns=["source", "consumer_type", "price_component_or_level"],
        values="price_euro",
        join_column_levels_with="-",
    )
    # Table for price components (in PPS) of gas and electricity prices of household and non-household consumers.
    tb_price_components_pps = tb[tb["dataset_code"].isin(DATASET_CODES_COMPONENTS)].pivot(
        index=["country", "year"],
        columns=["source", "consumer_type", "price_component_or_level"],
        values="price_pps",
        join_column_levels_with="-",
    )
    # Improve tables format.
    tb_price_components_euro = tb_price_components_euro.format(
        ["country", "year"], short_name="gas_and_electricity_price_components_euro_flat"
    )
    tb_price_components_pps = tb_price_components_pps.format(
        ["country", "year"], short_name="gas_and_electricity_price_components_pps_flat"
    )

    # Improve column names.
    tb_price_components_euro = tb_price_components_euro.rename(
        columns={column: column.replace("__", "_") + "_euro" for column in tb_price_components_euro.columns},
        errors="raise",
    )
    tb_price_components_pps = tb_price_components_pps.rename(
        columns={column: column.replace("__", "_") + "_pps" for column in tb_price_components_pps.columns},
        errors="raise",
    )

    return tb_prices_euro, tb_prices_pps, tb_price_components_euro, tb_price_components_pps


def sanity_check_outputs(tb: Table) -> None:
    error = "Expected 'Energy and supply' and 'Network costs' to be non-negative."
    assert tb[
        tb["dataset_code"].isin(DATASET_CODES_COMPONENTS)
        & tb["price_component_or_level"].isin(["Energy and supply", "Network costs"])
        & (tb["price_euro"] < 0)
    ].empty, error

    # Further sanity checks on component prices.
    tb_components = tb[tb["dataset_code"].isin(DATASET_CODES_COMPONENTS)].reset_index(drop=True)
    tb_taxes_sum = (
        tb_components[
            tb_components["price_component_or_level"].isin(
                [
                    "Capacity taxes",
                    "Environmental taxes",
                    "Nuclear taxes",
                    "Renewable taxes",
                    "Value added tax (VAT)",
                    "Other",
                ]
            )
        ]
        .groupby(["dataset_code", "country", "year"], observed=True, as_index=False)
        .agg({"price_euro": "sum"})
    )
    tb_taxes_original = tb_components[
        tb_components["price_component_or_level"] == "Taxes, fees, levies, and charges"
    ].reset_index(drop=True)[["dataset_code", "country", "year", "price_euro"]]
    # NOTE: The median value of the sum is 0.0191 euros/kWh. When comparing the percentage difference, ignore values that are too small.
    compared = tb_taxes_sum.merge(
        tb_taxes_original, how="outer", on=["dataset_code", "country", "year"], suffixes=("_sum", "_original")
    )
    compared["dev"] = 100 * (
        abs(compared["price_euro_sum"] - compared["price_euro_original"]) / compared["price_euro_original"]
    )
    error = "Expected the sum of 'Capacity taxes', 'Environmental taxes', 'Nuclear taxes', 'Renewable taxes', 'Value added tax (VAT)', 'Other' to coincide with 'Taxes, fees, levies, and charges', within 2% (ignoring any prices below 0.007, which is 17% of rows)."
    # NOTE: Some dataset-country-year have a significant discrepancy, e.g. nrg_pc_202_c-Greece-2022, with a price of 6.7€/MWh.
    assert compared[(compared["price_euro_original"] > 7) & (compared["dev"] > 2)].empty, error
    # compared.sort_values("dev", ascending=False).head(60)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its table.
    ds_meadow = paths.load_dataset("gas_and_electricity_prices")
    tb = ds_meadow.read("gas_and_electricity_prices")

    # Load HICP data and read its table.
    ds_hicp = paths.load_dataset("harmonised_index_of_consumer_prices")
    tb_hicp = ds_hicp.read("harmonised_index_of_consumer_prices")

    #
    # Process data.
    #
    # Select relevant dataset codes, and add a column with the dataset name.
    tb = tb[tb["dataset_code"].isin(DATASET_CODES_AND_NAMES.keys())].reset_index(drop=True)

    # Select and rename columns.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Sanity checks on inputs.
    sanity_check_inputs(tb=tb)

    # Clean inputs.
    tb = prepare_inputs(tb=tb)

    # Harmonize indexes and country names.
    tb = harmonize_indexes_and_countries(tb=tb)

    # Select and prepare relevant data.
    tb = select_and_prepare_relevant_data(tb=tb)

    # We want to use the HICP data to adjust the prices for inflation.
    # There are many options in the "Classification of individual consumption by purpose (COICOP)" column.
    # As recommended by Eurostat, we will use only the following:
    # [TOT_X_NRG] Overall index excluding energy
    # Excluding the energy component helps in depicting the prices adjusted for the underlying inflationary trends, without being distorted by the volatility typically associated with energy commodities. In this respect, adjusted prices will include the impact of the energy cost changes.
    tb_hicp = tb_hicp[tb_hicp["classification"] == "TOT_X_NRG"].reset_index(drop=True)

    # Calculate average HICP for each country and year.
    tb_hicp["year"] = tb_hicp["date"].str[0:4].astype(int)
    tb_hicp = tb_hicp.groupby(["country", "year"], observed=True, as_index=False).agg({"hicp": "mean"})

    # Add HICP column to main table.
    tb = tb.merge(tb_hicp, how="left", on=["country", "year"])

    # Adjust prices for inflation.
    tb["price_euro"] = tb["price_euro"] / tb["hicp"] * 100
    tb["price_pps"] = tb["price_pps"] / tb["hicp"] * 100

    # Sanity check outputs.
    sanity_check_outputs(tb=tb)

    # Uncomment to plot a comparison (for each country, source, and consumer type) between the prices and the components data.
    # NOTE: Some of the biggest discrepancies happen where prices data is given only for one of the semesters. This is the case of Georgia household electricity in 2021 and 2022, where we can't see the value of the missing semester (which could explain why the components data is significantly higher).
    # plot_final_comparison_between_prices_and_components_data(tb=tb)

    # Create convenient wide tables.
    tb_prices_euro, tb_prices_pps, tb_price_components_euro, tb_price_components_pps = prepare_wide_tables(tb=tb)

    # Improve main table format.
    tb = tb.drop(columns=["dataset_code"]).format(
        ["country", "date", "source", "consumer_type", "price_component_or_level"]
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb, tb_prices_euro, tb_prices_pps, tb_price_components_euro, tb_price_components_pps],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )
    ds_garden.save()
