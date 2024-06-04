"""Load a garden dataset and create a grapher dataset."""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def linear_pop_estimates(start_row, end_row):
    """Linearly estimate population values between two rows."""
    start_year = start_row["year"]
    end_year = end_row["year"]
    start_pop = start_row["population"]
    end_pop = end_row["population"]

    # Calculate the slope and intercept of the linear equation.
    slope = (end_pop - start_pop) / (end_year - start_year)
    intercept = start_pop - slope * start_year

    # Calculate the population values for the years between start_year and end_year.
    years = list(range(start_year + 1, end_year))
    pops = [slope * year + intercept for year in years]

    return [{"year": year, "population": pop} for year, pop in zip(years, pops)]


def estimate_pops(tb_pop):
    pops_ls = []
    for idx, row in tb_pop.iterrows():
        if idx == tb_pop.index[-1]:
            break
        pops_ls.extend(linear_pop_estimates(row, tb_pop.loc[idx + 1]))
    return pops_ls


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.garden_dataset

    # Read table from garden dataset.
    tb = ds_garden["cigarette_sales"].reset_index()

    # Process data.

    # load population data
    # ds_population = paths.load_dataset("population")

    # linearly estimate population values for East and West Germany

    # e_ger = tb_pop[(tb_pop["country"] == "East Germany") & (tb_pop["year"] > 1900)]
    # w_ger = tb_pop[(tb_pop["country"] == "West Germany") & (tb_pop["year"] > 1900)]

    # e_ger_pop = pd.DataFrame(estimate_pops(e_ger))
    # e_ger_pop["country"] = "East Germany"
    # w_ger_pop = pd.DataFrame(estimate_pops(w_ger))
    # w_ger_pop["country"] = "West Germany"

    # germany_pop = Table(pd.concat([e_ger_pop, w_ger_pop]))
    # germany_pop = germany_pop.format(["country", "year"])

    # germany_tb = tb[tb["country"].isin(["West Germany", "East Germany"])].format(["country", "year"])

    # germany_tb = geo.add_population_to_table(germany_tb, ds_population, interpolate_missing_population=True)

    # include West Germany values for Germany 1945-1990
    tb = tb.replace("West Germany", "Germany")

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    tb = tb.format(["country", "year"])

    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
