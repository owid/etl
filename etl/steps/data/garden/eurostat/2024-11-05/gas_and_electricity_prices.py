"""Load a meadow dataset and create a garden dataset."""
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
        "c": "confidential",
        "d": "definition differs",
        "b": "break in time series",
        "p": "provisional",
        "u": "low reliability",
        "cd": "confidential, definition differs",
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
    },
    # Energy units.
    "energy_unit": {
        # TODO: Confirm this definition (the page wasn't working).
        "GJ_GCV": "GJ - Gross Calorific Value",
        "KWH": "kWh",
        # The following is used in consumption volumes datasets.
        # "PC": "Percentage",
    },
}


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


def clean_inputs(tb: Table) -> Table:
    # Values sometimes include a letter, which is a flag. Extract those letters and create a separate column with them.
    # Note that sometimes there can be multiple letters (which means multiple flags).
    tb["flag"] = tb["value"].astype("string").str.extract(r"([a-z]+)", expand=False)
    tb["value"] = tb["value"].str.replace(r"[a-z]", "", regex=True)

    # Some values are start with ':' (namely ':', ': ', ': c', ': u', ': cd'). Replace them with nan.
    tb.loc[tb["value"].str.startswith(":"), "value"] = None

    # Assign a proper type to the column of values.
    tb["value"] = tb["value"].astype(float)

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


def compare_components_and_prices_data(tb: Table) -> None:
    # Ideally, the prices obtained by adding up all components (in the components dataset) should be similar to those obtained in the prices dataset.
    # However, both are very sparse (especially the prices dataset), and the prices dataset is also given in semesters, which makes it difficult to compare (without having the actual consumption of each semester).
    dataset_components = "nrg_pc_204_c"
    dataset_prices = "nrg_pc_204"
    # Transform biannual data into annual data, by taking the average price of both semesters.
    # NOTE: It would be better to have the actual energy consumption of each semester, to be able to compute a weighted average. But I don't see that data (we might need to import some additional datasets). For now, simply take the average.
    tb_biannual = tb[tb["year"].str.contains("S")].reset_index(drop=True)
    tb_biannual["year"] = tb_biannual["year"].str.strip().str[0:4].astype(int)
    # Compute an annual average only if there is data for the two semesters.
    tb_biannual_filtered = tb_biannual.groupby(
        ["currency", "country", "year", "dataset_code", "dataset_name", "price_component_or_level"],
        observed=True,
        as_index=False,
    ).filter(lambda x: len(x) == 2)
    tb_biannual = tb_biannual_filtered.groupby(
        ["currency", "country", "year", "dataset_code", "dataset_name", "price_component_or_level"],
        observed=True,
        as_index=False,
    ).agg({"value": "mean"})

    # Check that the resulting total price for the components dataset (summing up all components) is similar to the electricity prices averaged over the two semesters.
    tb_annual = tb[~tb["year"].str.contains("S")].reset_index(drop=True)
    tb_annual["year"] = tb_annual["year"].str.strip().str[0:4].astype(int)

    # OPTION 1: Sum over all price components to get the total.
    annual_1 = (
        tb_annual[(tb_annual["currency"] == "Euro") & (tb_annual["dataset_code"] == dataset_components)]
        .groupby(
            ["country", "year"],
            observed=True,
            as_index=False,
        )
        .agg({"value": lambda x: x.sum(min_count=1)})
        .dropna()
        .reset_index(drop=True)
    )

    # OPTION 2: Choose one of the price levels ({'All taxes and levies included', 'Excluding VAT and other recoverable taxes and levies', 'Excluding taxes and levies').
    price_level = "All taxes and levies included"
    # price_level = "Excluding VAT and other recoverable taxes and levies"
    # price_level = "Excluding taxes and levies"
    annual_2 = tb_biannual[
        (tb_biannual["currency"] == "Euro")
        & (tb_biannual["dataset_code"] == dataset_prices)
        & (tb_biannual["price_component_or_level"] == price_level)
    ][["country", "year", "value"]]
    compared = annual_1.merge(annual_2, how="inner", on=["country", "year"], suffixes=("_components", "_prices"))
    # Only a few country-years could be compared this way. Most of the points in the prices datasets were missing.
    import plotly.express as px

    for country in sorted(set(compared["country"])):
        px.line(
            compared[compared["country"] == country].melt(id_vars=["country", "year"]),
            x="year",
            y="value",
            color="variable",
            markers=True,
            title=country,
        ).show()

    # Also check the percentage deviation.
    # compared["dev"] = 100 * abs(compared["value_components"] - compared["value_prices"]) / compared["value_prices"]

    # Conclusions:
    # * When consumption band "All bands" is selected, there are very few points where both curves (prices and components) can be compared. When choosing another band, e.g. "<20GJ", there are more points to compare (in the case of gas).
    # * The annual components prices (summed over all components) tends to be systematically higher than the biannual prices (averaged over the two semester of the year). I suppose this is caused by doing a simple average instead of weighting by consumption. In semesters with higher consumption (e.g. winter), the increased demand tends to drive prices up. Annual prices, as far as I understand, are consumption-weighted averages, and therefore assign a larger weight to those semesters with higher prices. So, intuitively, it makes sense that the true annual prices tend to be higher than the averaged biannual prices.

    # With all this, it seems reasonable to discard the prices datasets, and use the components ones.


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gas_and_electricity_prices")

    # Read table from meadow dataset.
    tb = ds_meadow.read_table("gas_and_electricity_prices")

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
    tb = clean_inputs(tb=tb)

    # Harmonize indexes and country names.
    tb = harmonize_indexes_and_countries(tb=tb)

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

        # For convenience, instead of having a column for price component (for components datasets) and price level (for prices datasets), create a single column with the price component or level.
        assert tb[(tb["price_level"].isnull()) & (tb["price_component"].isnull())].empty
        assert tb[(tb["price_level"].notnull()) & (tb["price_component"].notnull())].empty
        tb["price_component_or_level"] = tb["price_level"].fillna(tb["price_component"])
        tb = tb.drop(columns=["price_level", "price_component"], errors="raise")

        # After inspection, it looks like the "All bands" consumption is very sparse in the prices datasets.
        # One option (if we decided to use the prices dataset) would be to use the more common consumption bands only, which are better informed.
        # However, "All bands" in the components dataset seem to be less sparse (at least from 2019 onwards).
        # To get the total price from the components dataset, we would need to add up all components.
        # After a quick inspection, all components seem similarly well informed.
        # There are some special cases where the data is omitted (e.g. due to flag "c" (confidential)).
        # So, in principle we could add up all components, only when all of them are informed.
        # TODO: Reconsider this choice if this loses much data, e.g. when some components are missing simply because they are irrelevant for a specific country.
        # For now, select "All bands" and the components datasets.
        tb = (
            tb[tb["consumption_band"] == "All bands"]
            .drop(columns=["consumption_band"], errors="raise")
            .reset_index(drop=True)
        )

        # Check that the total price obtained by summing all components is similar to the price obtained in the prices dataset.
        # NOTE: Uncomment to perform some visual checks.
        #  See conclusions in the following function to understand the choices.
        # compare_components_and_prices_data(tb=tb)

        # Select the components datasets only.
        tb = tb[tb["dataset_code"].isin(["nrg_pc_202_c", "nrg_pc_203_c", "nrg_pc_204_c", "nrg_pc_205_c"])].reset_index(
            drop=True
        )

        return tb

    # Select and prepare relevant data.
    tb = select_and_prepare_relevant_data(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
