"""Match variable IDs from and old version of a dataset to the analogous variables in the new version of the dataset.

After a dataset has been uploaded to OWID's MySQL database, we need to pair new variable IDs with the old ones,
so that all graphs update properly. If the variable names are identical, the task is trivial: find indexes of old
variables and map them to the indexes of the identical variables in the new dataset.
However, if variable names have changed (or the number of variables have changed) the pairing may need to be done
manually. This script is a CLI tool that may help in either scenario.

"""

import json
import os
from typing import Any, Callable

import click
import pandas as pd
from fuzzywuzzy import fuzz

from etl.db import get_connection, get_dataset_id, get_variables_in_dataset

# True to skip variables that are identical in old and new datasets, when running comparison.
# If so, identical variables will be matched automatically.
# False to include variables with identical names in comparison.
OMIT_IDENTICAL = True
# Maximum number of suggested variables to display when fuzzy matching an old variable.
N_MAX_SUGGESTIONS = 10
# Name of default similarity function to use to match old and new variables.
SIMILARITY_NAME = "partial_ratio"
# Similarity methods currently considered.
SIMILARITY_NAMES = {
    "ratio": fuzz.ratio,
    "partial_ratio": fuzz.partial_ratio,
    "partial_token_set_ratio": fuzz.partial_token_set_ratio,
    "partial_token_sort_ratio": fuzz.partial_token_sort_ratio,
    "token_set_ratio": fuzz.token_set_ratio,
    "token_sort_ratio": fuzz.token_sort_ratio,
}


def get_similarity_function(similarity_name: str) -> Any:
    """Return a similarity function given its name.

    Parameters
    ----------
    similarity_name : str
        Name of similarity function.

    Returns
    -------
    Callable :
        Similarity function.

    """
    if similarity_name in SIMILARITY_NAMES:
        similarity_function = SIMILARITY_NAMES[similarity_name]
    else:
        raise ValueError(f"ERROR: Unknown similarity function: {similarity_name}")

    return similarity_function


def save_data_to_json_file(data: list[str], json_file: str, **kwargs: Any) -> None:
    """Save data to a json file.

    Parameters
    ----------
    data : list or dict
        Data.
    json_file : str
        Path to json file.
    **kwargs
        Additional keyword arguments to pass to json dump function (e.g. indent=4, sort_keys=True).

    """
    output_dir = os.path.dirname(os.path.abspath(json_file))
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    with open(json_file, "w") as _json_file:
        json.dump(data, _json_file, **kwargs)


def _display_compared_variables(
    old_name: str,
    new_name: str,
    missing_new: pd.DataFrame,
    n_max_suggestions: int = N_MAX_SUGGESTIONS,
) -> None:
    click.secho(f"\nOld variable: {old_name}", fg="red", bold=True, blink=True)
    click.secho(f"New variable: {new_name}", fg="green", bold=True)
    click.secho("\n\tOther options:", italic=True)
    for i, row in missing_new.iloc[1 : 1 + n_max_suggestions].iterrows():
        click.secho(
            f"\t{i:5} - {row['name_new']} (id={row['id_new']},"
            f" similarity={row['similarity']:.0f})",
            fg="bright_green",
        )
    click.echo("\n")


def _input_manual_decision(new_indexes: list[Any]) -> Any:
    decision = input(
        "> Press enter to accept this option, or type chosen index. To ignore this"
        " variable, type i."
    )
    if decision == "":
        chosen_index = new_indexes[0]
    elif decision.lower() == "i":
        chosen_index = -1
    elif not decision.isdigit():
        chosen_index = None
    elif int(decision) in new_indexes:
        chosen_index = int(decision)
    else:
        chosen_index = None

    if chosen_index is None:
        print(f"Invalid option: It should be one in {new_indexes}.")

    return chosen_index


def map_old_and_new_variables(
    old_variables: pd.DataFrame,
    new_variables: pd.DataFrame,
    max_suggestions: int,
    omit_identical: bool = True,
    matching_function: Callable[[str, str], int] = fuzz.partial_ratio,
) -> pd.DataFrame:
    """Map old variables to new variables, either automatically (when they match perfectly) or manually.

    Parameters
    ----------
    old_variables : pd.DataFrame
        Table of old variable names (column 'name') and ids (column 'id').
    new_variables : pd.DataFrame
        Table of new variable names (column 'name') and ids (column 'id').
    omit_identical : bool
        True to automatically match variables that have identical names in both datasets. False to include them in the
        manual comparison.
    matching_function : function
        Method to calculate the similarity between two terms.

    Returns
    -------
    mapping : pd.DataFrame
        Mapping table from old variable name and id to new variable name and id.

    """
    # Prepare dataframes of old and new variables.
    old_variables = old_variables[["id", "name"]].rename(
        columns={"id": "id_old", "name": "name_old"}
    )
    new_variables = new_variables[["id", "name"]].rename(
        columns={"id": "id_new", "name": "name_new"}
    )
    # Find variables with identical names in old and new dataset (optionally).
    if omit_identical:
        mapping = pd.merge(
            old_variables,
            new_variables,
            left_on="name_old",
            right_on="name_new",
            how="inner",
        )
        names_to_omit = mapping["name_old"].tolist()
    else:
        mapping = pd.DataFrame()
        names_to_omit = []

    # Prepare dataframe of variables to sweep through in old and new datasets.
    missing_old = old_variables[
        ~old_variables["name_old"].isin(names_to_omit)
    ].reset_index(drop=True)
    missing_new = new_variables[
        ~new_variables["name_new"].isin(names_to_omit)
    ].reset_index(drop=True)

    # Iterate over old variables, and find the right match among new variables.
    num_variables = len(missing_old)
    count = 0
    while len(missing_old) > 0:
        count += 1
        # Indexes of the old dataframe.
        old_indexes = missing_old.index.tolist()
        # Choose variable on the first row of the old dataframe.
        current_old_index = old_indexes[0]
        old_name = missing_old.loc[current_old_index]["name_old"]
        old_index = missing_old.loc[current_old_index]["id_old"]

        # Sort new variables from most to least similar to current variable.
        missing_new["similarity"] = [
            matching_function(old_name, new_name)
            for new_name in missing_new["name_new"]
        ]
        missing_new = missing_new.sort_values("similarity", ascending=False)

        # Indexes of the new dataframe.
        new_indexes = missing_new.index.tolist()
        # By default, choose the variable with the highest similarity to the old one.
        suggested_index = new_indexes[0]
        new_name = missing_new.loc[suggested_index]["name_new"]

        # Display comparison.
        click.secho(
            f"\nVARIABLE {count}/{num_variables}", bold=True, bg="white", fg="black"
        )
        _display_compared_variables(
            old_name=old_name,
            new_name=new_name,
            missing_new=missing_new,
            n_max_suggestions=max_suggestions,
        )

        # Get chosen option from manual input.
        chosen_index = _input_manual_decision(new_indexes)

        if chosen_index is not None:
            if chosen_index == -1:
                missing_old = missing_old.drop(current_old_index)
            else:
                new_name = missing_new.loc[chosen_index]["name_new"]
                new_index = missing_new.loc[chosen_index]["id_new"]
                missing_old = missing_old.drop(current_old_index)
                missing_new = missing_new.drop(chosen_index)
                mapping_added = pd.DataFrame(
                    {
                        "id_old": [old_index],
                        "name_old": [old_name],
                        "id_new": [new_index],
                        "name_new": [new_name],
                    }
                )
                mapping = pd.concat([mapping, mapping_added], ignore_index=True)

    return mapping


def display_summary(
    old_variables: pd.DataFrame, new_variables: pd.DataFrame, mapping: pd.DataFrame
) -> None:
    """Display summary of the result of the mapping.

    Parameters
    ----------
    old_variables : pd.DataFrame
        Table of old variable names (column 'name') and ids (column 'id').
    new_variables : pd.DataFrame
        Table of new variable names (column 'name') and ids (column 'id').
    mapping : pd.DataFrame
        Mapping table from old variable name and id to new variable name and id.

    """
    print("Matched pairs:")
    for i, row in mapping.iterrows():
        print(f"\n  {row['name_old']} ({row['id_old']})")
        print(f"  {row['name_new']} ({row['id_new']})")

    unmatched_old = old_variables[
        ~old_variables["name"].isin(mapping["name_old"])
    ].reset_index(drop=True)
    unmatched_new = new_variables[
        ~new_variables["name"].isin(mapping["name_new"])
    ].reset_index(drop=True)
    if len(unmatched_old) > 0:
        print("\nUnmatched variables in the old dataset:")
        for i, row in unmatched_old.iterrows():
            print(f"  {row['name']} ({row['id']})")
    else:
        print("\nAll variables in the old dataset have been matched.")
    if len(unmatched_new) > 0:
        print("\nUnmatched variables in the new dataset:")
        for i, row in unmatched_new.iterrows():
            print(f"  {row['name']} ({row['id']})")
    else:
        print("\nAll variables in the new dataset have been matched.")


def save_variable_replacements_file(mapping: pd.DataFrame, output_file: str) -> None:
    """Save a json file with the mapping from old to new variable ids.

    Parameters
    ----------
    mapping : pd.DataFrame
        Mapping table from old variable name and id to new variable name and id.
    output_file : str
        Path to output file.

    """
    # Create a dictionary mapping from old variable id to new variable id.
    mapping_indexes = (
        mapping[["id_old", "id_new"]].set_index("id_old").to_dict()["id_new"]
    )
    mapping_indexes = {str(key): str(mapping_indexes[key]) for key in mapping_indexes}

    print(f"Saving index mapping to json file: {output_file}")
    save_data_to_json_file(
        data=mapping_indexes, json_file=output_file, **{"indent": 4, "sort_keys": True}
    )


def main(
    old_dataset_name: str,
    new_dataset_name: str,
    output_file: str,
    omit_identical: bool = OMIT_IDENTICAL,
    similarity_name: str = SIMILARITY_NAME,
    max_suggestions: int = N_MAX_SUGGESTIONS,
) -> None:
    with get_connection() as db_conn:
        # Get old and new dataset ids.
        old_dataset_id = get_dataset_id(db_conn=db_conn, dataset_name=old_dataset_name)
        new_dataset_id = get_dataset_id(db_conn=db_conn, dataset_name=new_dataset_name)

        # Get variables from old dataset that have been used in at least one chart.
        old_variables = get_variables_in_dataset(
            db_conn=db_conn, dataset_id=old_dataset_id, only_used_in_charts=True
        )
        # Get all variables from new dataset.
        new_variables = get_variables_in_dataset(
            db_conn=db_conn, dataset_id=new_dataset_id, only_used_in_charts=False
        )

    # Select similarity function.
    similarity_function = get_similarity_function(similarity_name)

    # Manually map old variable names to new variable names.
    mapping = map_old_and_new_variables(
        old_variables=old_variables,
        new_variables=new_variables,
        omit_identical=omit_identical,
        matching_function=similarity_function,
        max_suggestions=max_suggestions,
    )

    # Display summary.
    display_summary(
        old_variables=old_variables, new_variables=new_variables, mapping=mapping
    )

    # Save mapping to json file.
    save_variable_replacements_file(mapping, output_file)


@click.command(help=__doc__)
@click.option(
    "-f",
    "--output-file",
    type=str,
    help="Path to output json file.",
    required=True,
)
@click.option(
    "-old",
    "--old-dataset-name",
    type=str,
    help="Old dataset name (as defined in grapher).",
    required=True,
)
@click.option(
    "-new",
    "--new-dataset-name",
    type=str,
    help="New dataset name (as defined in grapher).",
    required=True,
)
@click.option(
    "-s",
    "--similarity-name",
    type=str,
    default=SIMILARITY_NAME,
    help=(
        "Name of similarity function to use when fuzzy matching variables."
        f" Default: {SIMILARITY_NAME}. Available methods:"
        f" {', '.join(list(SIMILARITY_NAMES))}."
    ),
)
@click.option(
    "-a",
    "--add-identical-pairs",
    is_flag=True,
    default=False,
    help=(
        "If given, add variables with identical names in both datasets to the"
        " comparison. If not given, omit such variables and assume they should be"
        " paired."
    ),
)
@click.option(
    "-m",
    "--max-suggestions",
    type=int,
    default=N_MAX_SUGGESTIONS,
    help=(
        "Number of suggestions to show per old variable. That is, for every old"
        " variable at most [--max-suggestions] suggestions will be listed."
    ),
)
def main_cli(
    old_dataset_name: str,
    new_dataset_name: str,
    output_file: str,
    add_identical_pairs: bool,
    similarity_name: str,
    max_suggestions: int,
) -> None:
    main(
        old_dataset_name=old_dataset_name,
        new_dataset_name=new_dataset_name,
        omit_identical=not add_identical_pairs,
        similarity_name=similarity_name,
        output_file=output_file,
        max_suggestions=int(max_suggestions),
    )
