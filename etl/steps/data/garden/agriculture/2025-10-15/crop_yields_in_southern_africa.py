"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from each table, and how to rename them.
COLUMNS_MODIS = {
    "country": "country",
    "year": "year",
    "meangcvi": "yield",
}
COLUMNS_SIF = {
    "country_na": "country",
    "year": "year",
    "mean": "yield",
}
COLUMNS_FAO = {
    "area": "country",
    "year": "year",
    "element": "element",
    "item": "item",
    "value": "yield",
    # Just for sanity checks.
    "unit": "unit",
    "flag_description": "flag",
}


def adapt_and_combine_tables(tb_fao, tb_modis, tb_sif):
    # Sanity check.
    assert set(tb_fao["unit"]) == {"100 g/ha"}, "Unexpected units"
    assert set(tb_fao["flag"]) == {"Official figure", "Estimated value"}, "Unexpected flags"
    # Prepare FAO data.
    # There are two items in FAO data, namely "Maize (corn)" and "Green corn (maize)".
    # For now, use one of them (namely "Maize (corn)", which seems to be the one reported in Fig 1).
    tb_fao = tb_fao[(tb_fao["item"] == "Maize (corn)")][["country", "year", "yield"]].reset_index(drop=True)
    # Convert FAOSTAT yield units from 100 g/ha to tonnes/ha.
    tb_fao["yield"] /= 10000

    # Concatenate tables.
    tb = pr.concat(
        [
            tb_modis.assign(**{"source": "yield_modis"}),
            tb_sif.assign(**{"source": "yield_sif"}),
            tb_fao.assign(**{"source": "yield_fao"}),
        ],
        ignore_index=True,
    )

    return tb


def plot_curves(tb):
    import plotly.express as px

    plot = tb[["country", "year", "yield_fao", "yield_modis_rescaled", "yield_sif_rescaled"]].melt(
        id_vars=["country", "year"]
    )
    for country in sorted(set(plot["country"])):
        px.line(
            plot[plot["country"] == country],
            x="year",
            y="value",
            color="variable",
            title=country,
            color_discrete_map={
                "yield_fao": "red",
                "yield_sif_rescaled": "green",
                "yield_modis_rescaled": "blue",
            },
        ).show()


def shift_satellite_data(tb):
    """Shift satellite data one year forward, to align with FAO's way of reporting data by harvesting year.

    Corn grows in spring and is harvested in autumn. For countries in the Northern Hemisphere, both events happen during the same calendar year. However, for countries in the Southern Hemisphere, harvesting happens the following year. Given that FAOSTAT reports by harvesting year, we shift satellite data for countries in the Southern Hemisphere one year forward. This way, satellite and FAOSTAT data refer to the same crop season.

    NOTE: This seems to be the approach followed by the authors of the paper to compare satellite with FAO data, as shown in Fig 1.
    """
    tb_satellite = tb[["country", "year", "yield_modis", "yield_sif"]].copy()
    tb_satellite["year"] += 1
    tb = tb.drop(columns=["yield_modis", "yield_sif"]).merge(tb_satellite, on=["country", "year"], how="outer")

    return tb


def add_rescaled_columns(tb):
    # In the paper, the MODIS and SIF time series for each country are rescaled using a linear transformation so that their minimum and maximum values matched those of the corresponding FAO maize yields, enabling direct visual comparison of trends across datasets.
    # Add columns of yield rescaled to be able to compare them (as done for Fig 1 of the paper).
    for country, group in tb.groupby("country"):
        # Find absolute minimum and maximum of each source, for the current country.
        fao_min, fao_max = group["yield_fao"].min(), group["yield_fao"].max()
        modis_min, modis_max = group["yield_modis"].min(), group["yield_modis"].max()
        sif_min, sif_max = group["yield_sif"].min(), group["yield_sif"].max()
        # Rescale MODIS and SIF so that their minimum and maxima coincide with those of FAO.
        tb.loc[tb["country"] == country, "yield_modis_rescaled"] = fao_min + (group["yield_modis"] - modis_min) * (
            fao_max - fao_min
        ) / (modis_max - modis_min)
        tb.loc[tb["country"] == country, "yield_sif_rescaled"] = fao_min + (group["yield_sif"] - sif_min) * (
            fao_max - fao_min
        ) / (sif_max - sif_min)
    # Add metadata to the newly created columns.
    tb["yield_modis_rescaled"] = tb["yield_modis_rescaled"].copy_metadata(tb["yield_modis"])
    tb["yield_sif_rescaled"] = tb["yield_sif_rescaled"].copy_metadata(tb["yield_sif"])

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("crop_yields_in_southern_africa")

    # Read tables from meadow dataset.
    tb_modis = ds_meadow.read("crop_yields_in_southern_africa__modis")
    tb_fao = ds_meadow.read("crop_yields_in_southern_africa__fao")
    tb_sif = ds_meadow.read("crop_yields_in_southern_africa__sif")

    #
    # Process data.
    #
    # Select and rename columns from each table.
    tb_modis = tb_modis[list(COLUMNS_MODIS)].rename(columns=COLUMNS_MODIS, errors="raise")
    tb_sif = tb_sif[list(COLUMNS_SIF)].rename(columns=COLUMNS_SIF, errors="raise")
    tb_fao = tb_fao[list(COLUMNS_FAO)].rename(columns=COLUMNS_FAO, errors="raise")

    # Adapt tables and combine them.
    tb = adapt_and_combine_tables(tb_fao=tb_fao, tb_modis=tb_modis, tb_sif=tb_sif)

    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Create one column per source.
    tb = tb.pivot(index=["country", "year"], columns=["source"], values="yield", join_column_levels_with="_")

    # Shift satellite data one year forward, to align with FAO's way of reporting data by harvesting year.
    tb = shift_satellite_data(tb=tb)

    # To be able to reproduce the curves in Fig 1 of the paper, we need to add rescaled columns of satellite data.
    tb = add_rescaled_columns(tb=tb)

    # DEBUG: Uncomment to plot curves similar to those in Fig 1 of the paper.
    # plot_curves(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save garden dataset.
    ds_garden.save()
