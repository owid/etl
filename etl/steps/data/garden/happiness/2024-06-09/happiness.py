"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


REGIONS = {reg: reg_dict for reg, reg_dict in geo.REGIONS.items() if reg != "European Union (27)"}
REGIONS.update({"World": {}})


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset, previous years and population data.
    ds_meadow = paths.load_dataset("happiness", version="2024-06-09")
    ds_prev_years = paths.load_dataset("happiness", channel="garden", version="2023-03-20")
    ds_population = paths.load_dataset("population", channel="garden")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table datasets.
    tb_this_year = ds_meadow.read("happiness")
    tb_prev_years = ds_prev_years.read("happiness")

    # combine meadow data with previous years
    tb_this_year["cantril_ladder_score"] = tb_this_year["ladder_score"]
    cols_overlap = ["country", "cantril_ladder_score", "year"]
    tb = pr.concat([tb_this_year[cols_overlap], tb_prev_years], ignore_index=True)

    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Process data (add population weighted averages for continents & income groups)

    # save data of Northern Cyrpus and Somaliland to concat later (they do not have population in population dataset)
    countries_no_pop_msk = tb["country"].isin(["Northern Cyprus", "Somaliland"])
    tb_countries_wo_population = tb[countries_no_pop_msk]
    tb = tb[~countries_no_pop_msk]

    # add population to table
    tb = geo.add_population_to_table(tb, ds_population)

    # calculate population weighted averages by multiplying the population with the cantril ladder score
    # and then summing and dividing by the total population
    tb["cantril_times_pop"] = tb["cantril_ladder_score"] * tb["population"]

    aggr_score = {"cantril_times_pop": "sum", "population": "sum"}
    tb = geo.add_regions_to_table(
        tb,
        aggregations=aggr_score,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    # Divide the sum of the cantril ladder score times population by the total population
    tb["cantril_ladder_score"] = tb["cantril_times_pop"] / tb["population"]

    # drop unneeded columns
    tb = tb.drop(columns=["cantril_times_pop"])

    # add back Northern Cyprus and Somaliland
    tb = pr.concat([tb, tb_countries_wo_population], ignore_index=True)

    tb = tb.format(["country", "year"])

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the dataset
    ds_garden.save()
