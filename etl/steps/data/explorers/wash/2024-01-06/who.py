from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("who")
    tb = ds_garden["who"].reset_index()

    cols = tb.columns.drop(["country", "year", "residence"])
    tb = tb.pivot_table(index=["country", "year"], columns="residence", values=cols, aggfunc="first")

    tb.columns = ["_".join(col).strip().lower() for col in tb.columns.values]
    tb.columns = [modify_column_name(col) for col in tb.columns]

    # Create explorer dataset, with garden table and metadata in csv format
    ds_explorer = create_dataset(dest_dir, tables=[tb], formats=["csv"])
    ds_explorer.save()


def modify_column_name(column_name: str) -> str:
    """
    Modifying the column names to more closely match those used in the existing explorer
    """
    # Replace 'pop' with 'number'
    new_name = column_name.replace("pop", "number")
    # Remove '_total' from the column name
    new_name = new_name.replace("_total", "")
    return new_name
