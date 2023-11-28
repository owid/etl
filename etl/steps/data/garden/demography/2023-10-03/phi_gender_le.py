"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("life_tables")

    # Read table from meadow dataset.
    tb = ds_meadow["life_tables"].reset_index()

    #
    # Process data.
    #
    paths.log.info("replace 110+ -> 110, 100+ -> 100")
    tb["age"] = (
        tb["age"]
        .replace(
            {
                "110+": "110",
                "100+": "100",
            }
        )
        .astype(int)
    )

    # Keep only 1x1
    paths.log.info("keep only type='period' and sex in {'male', 'female'}")
    tb = tb[(tb["type"] == "period") & (tb["sex"].isin(["female", "male"]))].drop(columns=["type"])

    # Add phi
    paths.log.info("add phi parameter")
    tb = make_table_phi(tb)

    # Change short name
    tb.metadata.short_name = paths.short_name

    # Set index
    tb = tb.set_index(["location", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def make_table_phi(tb: Table) -> Table:
    """Estimate phi.

    Phi is defined as the outsurvival pronability of males (i.e. probability that a male will live longer than a female in a given population).

    This is estimated using Equation 2 from https://bmjopen.bmj.com/content/bmjopen/12/8/e059964.full.pdf.

    Inspired by code:
        - https://github.com/CPop-SDU/sex-gap-e0-pnas/tree/main
        - https://github.com/CPop-SDU/outsurvival-in-perspective
    """
    # Copy original metadata
    origins = tb["number_deaths"].metadata.origins

    # Calculate standard deviations
    tb["number_survivors"] = tb["number_survivors"] / 1e5
    tb["number_deaths"] = tb["number_deaths"] / 1e5

    # Pivot table to align males and females for the different metrics
    tb = tb.pivot(
        index=["location", "year", "age"], columns="sex", values=["number_survivors", "number_deaths"]
    ).reset_index()

    # Shift one up (note the subindex in the equation 'x-n', in our case n=1 (age group width))
    column = ("number_survivors", "male")
    tb[column] = tb.groupby(["location", "year"])[[column]].shift(-1).squeeze()

    # Estimate phi_i (i.e. Eq 2 for a specific age group, without the summation)
    tb["phi"] = (
        tb["number_deaths"]["female"] * tb["number_survivors"]["male"]
        + tb["number_deaths"]["female"] * tb["number_deaths"]["male"] / 2
    )
    # Apply the summation from Eq 2
    tb = tb.groupby(["location", "year"], as_index=False, observed=True)[[("phi", "")]].sum()

    # Scale
    tb["phi"] = (tb["phi"] * 100).round(2)

    # Fix column names (remove multiindex)
    tb.columns = [col[0] for col in tb.columns]

    # Copy metadata
    tb["phi"].metadata.origins = origins

    return tb
