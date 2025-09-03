import json
import os
from typing import Any, Dict, List, Tuple, Union, cast

import numpy as np
import pandas as pd
import rich_click as click
from rapidfuzz import fuzz
from structlog import get_logger

from apps.wizard.utils.db import WizardDB
from etl.db import get_connection
from etl.grapher.io import get_variables_in_dataset

# If True, identical variables will be matched automatically (by string comparison).
# If False, variables with identical names will appear in comparison.
MATCH_IDENTICAL = True
# Maximum number of suggested variables to display when fuzzy matching an old variable.
N_MAX_SUGGESTIONS = 10
# Name of default similarity function to use to match old and new variables.
SIMILARITY_NAME = "partial_ratio"
# Similarity methods currently considered.
SIMILARITY_NAMES = {
    "token_set_ratio": fuzz.token_set_ratio,
    "token_sort_ratio": fuzz.token_sort_ratio,
    "partial_ratio": fuzz.partial_ratio,
    "partial_token_set_ratio": fuzz.partial_token_set_ratio,
    "partial_token_sort_ratio": fuzz.partial_token_sort_ratio,
    "ratio": fuzz.ratio,
    "quick_ratio": fuzz.QRatio,
    "weighted_ratio": fuzz.WRatio,
}
log = get_logger()


@click.command(name="variable-match", help=__doc__)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print the mappings without applying them.",
)
@click.option(
    "-old",
    "--old-dataset-id",
    type=int,
    help="Old dataset ID (as defined in grapher).",
    required=True,
)
@click.option(
    "-new",
    "--new-dataset-id",
    type=int,
    help="New dataset ID (as defined in grapher).",
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
    default=False,  # TODO: we may want to change default behaviour to True
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
@click.option(
    "--no-interactive",
    is_flag=True,
    default=False,
    help=(
        "Skip interactive prompts and automatically map variables based on similarity threshold."
        " Best matches above the threshold will be selected automatically."
    ),
)
@click.option(
    "--auto-threshold",
    type=float,
    default=80.0,
    help="Similarity threshold (0-100) for automatic mapping when --no-interactive is used. Default: 80.0",
)
def main_cli(
    old_dataset_id: int,
    new_dataset_id: int,
    dry_run: bool,
    add_identical_pairs: bool,
    similarity_name: str,
    max_suggestions: int,
    no_interactive: bool,
    auto_threshold: float,
) -> None:
    """Match variable IDs from an old dataset to a new dataset's.

    After a dataset has been uploaded to OWID's MySQL database, we need to pair new variable IDs with the old ones,
    so that all graphs update properly.

    If the variable names are identical, the task is trivial: find indexes of old variables and map them to the indexes of the identical variables in the new dataset. However, if variable names have changed (or the number of variables have changed) the pairing may need to be done manually. This CLI helps in either scenario.
    """
    main(
        old_dataset_id=old_dataset_id,
        new_dataset_id=new_dataset_id,
        dry_run=dry_run,
        match_identical=not add_identical_pairs,
        similarity_name=similarity_name,
        max_suggestions=int(max_suggestions),
        no_interactive=no_interactive,
        auto_threshold=auto_threshold,
    )


def main(
    old_dataset_id: int,
    new_dataset_id: int,
    dry_run: bool,
    match_identical: bool = MATCH_IDENTICAL,
    similarity_name: str = SIMILARITY_NAME,
    max_suggestions: int = N_MAX_SUGGESTIONS,
    no_interactive: bool = False,
    auto_threshold: float = 80.0,
) -> None:
    with get_connection() as db_conn:
        # Get variables from old dataset that have been used in at least one chart.
        old_indicators = get_variables_in_dataset(db_conn=db_conn, dataset_id=old_dataset_id, only_used_in_charts=True)
        # Get all variables from new dataset.
        new_indicators = get_variables_in_dataset(db_conn=db_conn, dataset_id=new_dataset_id, only_used_in_charts=False)

    # Map old variable names to new variable names.
    if no_interactive:
        mapping = map_old_and_new_indicators_auto(
            old_indicators=old_indicators,
            new_indicators=new_indicators,
            match_identical=match_identical,
            similarity_name=similarity_name,
            auto_threshold=auto_threshold,
        )
    else:
        mapping = map_old_and_new_indicators(
            old_indicators=old_indicators,
            new_indicators=new_indicators,
            match_identical=match_identical,
            similarity_name=similarity_name,
            max_suggestions=max_suggestions,
        )

    # Display summary.
    display_summary(old_indicators=old_indicators, new_indicators=new_indicators, mapping=mapping)

    if dry_run:
        # Print the mappings and dataframe
        print_mappings(mapping)
        print_mapping_dataframe(mapping, old_dataset_id, new_dataset_id)
    else:
        # Save to MySQL database
        save_mappings_to_database(mapping, old_dataset_id, new_dataset_id)


def map_old_and_new_indicators(
    old_indicators: pd.DataFrame,
    new_indicators: pd.DataFrame,
    max_suggestions: int,
    match_identical: bool = True,
    similarity_name: str = "partial_ratio",
) -> pd.DataFrame:
    """Map old variables to new variables, either automatically (when they match perfectly) or manually.

    Parameters
    ----------
    old_indicators : pd.DataFrame
        Table of old variable names (column 'name') and ids (column 'id').
    new_indicators : pd.DataFrame
        Table of new variable names (column 'name') and ids (column 'id').
    match_identical : bool
        True to automatically match variables that have identical names in both datasets. False to include them in the
        manual comparison.
    similarity_name: str
        Similarity function name. Must be in `SIMILARITY_NAMES`.

    Returns
    -------
    mapping : pd.DataFrame
        Mapping table from old variable name and id to new variable name and id.

    """
    # get initial mapping
    mapping, missing_old, missing_new = preliminary_mapping(old_indicators, new_indicators, match_identical)
    # get suggestions for mapping
    suggestions = find_mapping_suggestions(missing_old, missing_new, similarity_name)
    # iterate over suggestions and get user feedback
    mapping = consolidate_mapping_suggestions_with_user(mapping, suggestions, max_suggestions)
    return mapping


def map_old_and_new_indicators_auto(
    old_indicators: pd.DataFrame,
    new_indicators: pd.DataFrame,
    match_identical: bool = True,
    similarity_name: str = "partial_ratio",
    auto_threshold: float = 80.0,
) -> pd.DataFrame:
    """Map old variables to new variables automatically based on similarity threshold.

    Parameters
    ----------
    old_indicators : pd.DataFrame
        Table of old variable names (column 'name') and ids (column 'id').
    new_indicators : pd.DataFrame
        Table of new variable names (column 'name') and ids (column 'id').
    match_identical : bool
        True to automatically match variables that have identical names in both datasets.
    similarity_name: str
        Similarity function name. Must be in `SIMILARITY_NAMES`.
    auto_threshold: float
        Minimum similarity score (0-100) to automatically create a mapping.

    Returns
    -------
    mapping : pd.DataFrame
        Mapping table from old variable name and id to new variable name and id.
    """
    # Get initial mapping (identical matches)
    mapping, missing_old, missing_new = preliminary_mapping(old_indicators, new_indicators, match_identical)

    # Get suggestions for mapping
    suggestions = find_mapping_suggestions(missing_old, missing_new, similarity_name)

    # Automatically map based on threshold
    mappings = [mapping]
    used_new_ids = set(mapping["id_new"].tolist())

    for suggestion in suggestions:
        name_old = suggestion["old"]["name_old"]
        id_old = suggestion["old"]["id_old"]
        candidates = suggestion["new"]

        # Filter out already used new indicators
        candidates = candidates[~candidates["id_new"].isin(used_new_ids)]

        if len(candidates) > 0:
            best_match = candidates.iloc[0]  # Already sorted by similarity
            similarity_score = best_match["similarity"]

            if similarity_score >= auto_threshold:
                # Add this mapping
                new_mapping = pd.DataFrame(
                    {
                        "id_old": [id_old],
                        "name_old": [name_old],
                        "id_new": [best_match["id_new"]],
                        "name_new": [best_match["name_new"]],
                    }
                )
                mappings.append(new_mapping)
                used_new_ids.add(best_match["id_new"])

                log.info(
                    f"Auto-mapped '{name_old}' -> '{best_match['name_new']}' (similarity: {similarity_score:.1f}%)"
                )

    # Combine all mappings
    if mappings:
        final_mapping = pd.concat(mappings, ignore_index=True)
    else:
        final_mapping = pd.DataFrame()

    return final_mapping


def display_summary(old_indicators: pd.DataFrame, new_indicators: pd.DataFrame, mapping: pd.DataFrame) -> None:
    """Display summary of the result of the mapping.

    Parameters
    ----------
    old_indicators : pd.DataFrame
        Table of old variable names (column 'name') and ids (column 'id').
    new_indicators : pd.DataFrame
        Table of new variable names (column 'name') and ids (column 'id').
    mapping : pd.DataFrame
        Mapping table from old variable name and id to new variable name and id.

    """
    print("Matched pairs:")
    for _, row in mapping.iterrows():
        print(f"\n  {row['name_old']} ({row['id_old']})")
        print(f"  {row['name_new']} ({row['id_new']})")

    unmatched_old = old_indicators[~old_indicators["name"].isin(mapping["name_old"])].reset_index(drop=True)
    unmatched_new = new_indicators[~new_indicators["name"].isin(mapping["name_new"])].reset_index(drop=True)
    if len(unmatched_old) > 0:
        print("\nUnmatched variables in the old dataset:")
        for _, row in unmatched_old.iterrows():
            print(f"  {row['name']} ({row['id']})")
    else:
        print("\nAll variables in the old dataset have been matched.")
    if len(unmatched_new) > 0:
        print("\nUnmatched variables in the new dataset:")
        for _, row in unmatched_new.iterrows():
            print(f"  {row['name']} ({row['id']})")
    else:
        print("\nAll variables in the new dataset have been matched.")


def print_mappings(mapping: pd.DataFrame) -> None:
    """Print the variable mappings in a readable format.

    Parameters
    ----------
    mapping : pd.DataFrame
        Mapping table from old variable name and id to new variable name and id.
    """
    if len(mapping) == 0:
        print("No variable mappings found.")
        return

    print("Variable Mappings:")
    print("=" * 60)
    for _, row in mapping.iterrows():
        print(f"Old: {row['name_old']} (ID: {row['id_old']})")
        print(f"New: {row['name_new']} (ID: {row['id_new']})")
        print("-" * 40)


def print_mapping_dataframe(mapping: pd.DataFrame, old_dataset_id: int, new_dataset_id: int) -> None:
    """Print the dataframe that would be saved to the database.

    Parameters
    ----------
    mapping : pd.DataFrame
        Mapping table from old variable name and id to new variable name and id.
    old_dataset_id : int
        ID of the old dataset.
    new_dataset_id : int
        ID of the new dataset.
    """
    if len(mapping) == 0:
        print("\nNo mappings to save to database.")
        return

    # Create the mapping dictionary that would be saved
    mapping_dict = mapping.set_index("id_old")["id_new"].to_dict()

    print("\nDataFrame that would be saved to database:")
    print("=" * 80)
    print(f"Dataset mapping: {old_dataset_id} -> {new_dataset_id}")
    print(f"Number of variable mappings: {len(mapping_dict)}")
    print("\nMapping dictionary:")
    for old_id, new_id in mapping_dict.items():
        print(f"  {old_id} -> {new_id}")


def save_mappings_to_database(mapping: pd.DataFrame, old_dataset_id: int, new_dataset_id: int) -> None:
    """Save variable mappings to the MySQL database.

    Parameters
    ----------
    mapping : pd.DataFrame
        Mapping table from old variable name and id to new variable name and id.
    old_dataset_id : int
        ID of the old dataset.
    new_dataset_id : int
        ID of the new dataset.
    """
    if len(mapping) == 0:
        print("No mappings to save to database.")
        return

    # Create the mapping dictionary expected by WizardDB.add_variable_mapping
    mapping_dict = {int(k): int(v) for k, v in mapping.set_index("id_old")["id_new"].to_dict().items()}

    # Save to database
    WizardDB.add_variable_mapping(
        mapping=mapping_dict,
        dataset_id_old=old_dataset_id,
        dataset_id_new=new_dataset_id,
        comments=f"Variable mapping from dataset {old_dataset_id} to {new_dataset_id}",
    )

    print(f"Successfully saved {len(mapping_dict)} variable mappings to database.")
    print(f"Dataset mapping: {old_dataset_id} -> {new_dataset_id}")


def preliminary_mapping(
    old_indicators: pd.DataFrame, new_indicators: pd.DataFrame, match_identical: bool
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Find initial mapping between old and new indicators.

    Builds a table with initial mapping, and two other dataframes with the remaining variables to be matched.
    Initial mapping is done by identical string comparison if `match_identical` is True. Otherwise it will be
    an empty dataframe.

    Parameters
    ----------
    old_indicators : pd.DataFrame
        Dataframe of old variables.
    new_indicators : pd.DataFrame
        Dataframe of new variables.
    match_identical : bool
        True to skip variables that are identical in old and new datasets, when running comparison.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
        Dataframes of old variables, new variables, and mapping between old and new variables.
    """
    # Prepare dataframes of old and new variables.
    old_indicators = old_indicators[["id", "name"]].rename(columns={"id": "id_old", "name": "name_old"})
    new_indicators = new_indicators[["id", "name"]].rename(columns={"id": "id_new", "name": "name_new"})

    # Find variables with identical names in old and new dataset.
    if match_identical:
        mapping = pd.merge(
            old_indicators,
            new_indicators,
            left_on="name_old",
            right_on="name_new",
            how="inner",
        )
        names_to_omit = mapping["name_old"].tolist()
        # Remove identically named variables from dataframes of variables to sweep through in old and new datasets.
        old_indicators = old_indicators[~old_indicators["name_old"].isin(names_to_omit)]
        new_indicators = new_indicators[~new_indicators["name_new"].isin(names_to_omit)]
    else:
        mapping = pd.DataFrame()

    old_indicators = old_indicators.reset_index(drop=True)
    new_indicators = new_indicators.reset_index(drop=True)

    return mapping, old_indicators, new_indicators


def find_mapping_suggestions(
    missing_old: pd.DataFrame,
    missing_new: pd.DataFrame,
    similarity_name: str = "partial_ratio",
) -> List[Dict[str, Any]]:
    """Find suggestions for mapping old variables to new variables.

    Creates a list with new variable suggestions for each old variable. The list is therefore the same
    size as len(old_indicators). Each item is a dictionary with two keys:

    - "old": Dictionary with old variable name and ID.
    - "new": pandas.DataFrame with new variable names, IDs, sorted by similarity to old variable name (according to matching_function).

    It uses the similiarity function `similarity_name` to estimate the score between `missing_old` and `missing_new`. Note that regardless of the score,
    if `missing_old` and `missing_new` have the same name, this will appear first with score 9999. (see _get_score internal function).

    Parameters
    ----------
    missing_old : pandas.DataFrame
        Dataframe with old variables.
    missing_new : pandas.DataFrame
        Dataframe with new variables.
    similarity_name : function, optional
        Similarity function name. The default is 'partial_ratio'. Must be in `SIMILARITY_NAMES`.

    Returns
    -------
    list
        List of suggestions for mapping old variables to new variables.
    """
    # get similarity function
    matching_function = get_similarity_function(similarity_name)

    def _get_score(old_name, new_name) -> float:
        """Get similarity score for a row.

        Uses matching_function, but on top of that ensures that score is maximum if names of old and new variables are identical.
        """
        if old_name == new_name:
            return 9999
        return matching_function(old_name, new_name)

    # Iterate over old variables, and find the right match among new variables.
    suggestions = []
    for _, row in missing_old.iterrows():
        # Old variable name
        old_name = row["name_old"]

        # Sort new variables from most to least similar to current variable.
        missing_new["similarity"] = [_get_score(old_name, new_name) for new_name in missing_new["name_new"]]
        missing_new = missing_new.sort_values("similarity", ascending=False)
        missing_new["similarity"] = missing_new["similarity"].apply(lambda x: min(x, 100))

        # Add results to suggestions list.
        suggestions.append(
            {
                "old": row.to_dict(),
                "new": missing_new.copy(),
            }
        )
    return suggestions


def find_mapping_suggestions_optim(
    missing_old: pd.DataFrame,
    missing_new: pd.DataFrame,
    similarity_name: str = "partial_ratio",
) -> List[Dict[str, Union[pd.DataFrame, pd.Series]]]:
    """Find suggestions for mapping old variables to new variables.

    Creates a list with new variable suggestions for each old variable. The list is therefore the same
    size as len(old_indicators). Each item is a dictionary with two keys:

    - "old": Dictionary with old variable name and ID.
    - "new": pandas.DataFrame with new variable names, IDs, sorted by similarity to old variable name (according to matching_function).

    It uses the similiarity function `similarity_name` to estimate the score between `missing_old` and `missing_new`. Note that regardless of the score,
    if `missing_old` and `missing_new` have the same name, this will appear first with score 9999. (see _get_score internal function).

    Parameters
    ----------
    missing_old : pandas.DataFrame
        Dataframe with old variables.
    missing_new : pandas.DataFrame
        Dataframe with new variables.
    similarity_name : function, optional
        Similarity function name. The default is 'partial_ratio'. Must be in `SIMILARITY_NAMES`.

    Returns
    -------
    list
        List of suggestions for mapping old variables to new variables.
    """
    # get similarity function
    matching_function = get_similarity_function(similarity_name)

    def _get_score(old_name, new_name) -> float:
        """Get similarity score for a row.

        Uses matching_function, but on top of that ensures that score is maximum if names of old and new variables are identical.
        """
        if old_name == new_name:
            return 9999
        return matching_function(old_name, new_name)

    # Vectorized approach to compute similarities (this needs adjustment based on matching_function capabilities)
    def compute_similarities_vectorized(old_names, new_names):
        # This function needs to be adapted to your specific similarity function
        # For example, using a vectorized string comparison if possible
        # Placeholder for actual implementation
        similarity_matrix = np.zeros((len(old_names), len(new_names)))
        for i, old_name in enumerate(old_names):
            for j, new_name in enumerate(new_names):
                similarity_matrix[i, j] = _get_score(old_name, new_name) if old_name != new_name else 9999
        return similarity_matrix

    # Pre-compute the similarity scores for all combinations
    old_names = missing_old["name_old"].to_numpy()
    new_names = missing_new["name_new"].to_numpy()
    similarity_scores = compute_similarities_vectorized(old_names, new_names)

    # Iterate over old variables to sort new variables based on pre-computed similarities
    suggestions = []
    for idx, old_row in missing_old.iterrows():
        # Retrieve the similarity scores for the current old_name
        scores = similarity_scores[idx]  # type: ignore

        # Sort missing_new based on these scores
        sorted_indices = np.argsort(-scores)  # Negative for descending sort
        sorted_missing_new = missing_new.iloc[sorted_indices].copy()
        sorted_missing_new["similarity"] = np.minimum(scores[sorted_indices], 100)

        # Add results to suggestions list
        suggestions.append(
            {
                "old": old_row.to_dict(),
                "new": sorted_missing_new,
            }
        )
    return suggestions


def consolidate_mapping_suggestions_with_user(
    mapping: pd.DataFrame, suggestions: List[Dict[str, Union[pd.Series, pd.DataFrame]]], max_suggestions: int
):
    """Consolidate mapping suggestions with user input.

    Given an initial mapping and a list of suggestions, this function prompts the user with options and consolidates the mapping
    based on their input."""
    count = 0
    ids_new_ignore = mapping["id_new"].tolist()
    mappings = [mapping]
    while len(suggestions) > 0:
        # always access last suggestion
        suggestion = suggestions[-1]
        count += 1

        # get relevant variables
        name_old = suggestion["old"]["name_old"]
        id_old = suggestion["old"]["id_old"]
        missing_new = suggestion["new"]
        missing_new = missing_new[~missing_new["id_new"].isin(ids_new_ignore)]  # type: ignore[reportCallIssue]
        new_indexes = missing_new.index.tolist()

        # display comparison to user
        click.secho(f"\nVARIABLE {count}/{len(suggestions)}", bold=True, bg="white", fg="black")
        _display_compared_variables(
            old_name=cast(str, name_old),
            missing_new=missing_new,
            n_max_suggestions=max_suggestions,
        )
        # get chosen option from manual input
        chosen_id = _input_manual_decision(new_indexes)

        # update mapping list
        if chosen_id is not None:
            # index not to be ignored
            if chosen_id != -1:
                # add mapping
                id_new = missing_new.loc[chosen_id]["id_new"]
                name_new = missing_new.loc[chosen_id]["name_new"]
                mappings.append(
                    pd.DataFrame(
                        {
                            "id_old": [id_old],
                            "name_old": [name_old],
                            "id_new": [id_new],
                            "name_new": [name_new],
                        }
                    )
                )
                # ignore the selected variable in the next suggestions
                ids_new_ignore.append(id_new)

            # forget suggestion once mapped or if user chose to ignore (chosen_id=-1)
            _ = suggestions.pop()

        mapping = pd.concat(mappings, ignore_index=True)

    return mapping


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


def save_data_to_json_file(data: Union[list[str], dict[Any, Any]], json_file: str, **kwargs: Any) -> None:
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
    missing_new: pd.DataFrame,
    n_max_suggestions: int = N_MAX_SUGGESTIONS,
) -> None:
    """Display final variable mapping summary in terminal."""
    new_name = missing_new.iloc[0]["name_new"]
    click.secho(f"\nOld variable: {old_name}", fg="red", bold=True, blink=True)
    click.secho(f"New variable: {new_name}", fg="green", bold=True)
    click.secho("\n\tOther options:", italic=True)
    for i, row in missing_new.iloc[1 : 1 + n_max_suggestions].iterrows():
        click.secho(
            f"\t{i:5} - {row['name_new']} (id={row['id_new']}, similarity={row['similarity']:.0f})",
            fg="bright_green",
        )
    click.echo("\n")


def _input_manual_decision(new_indexes: list[Any]) -> Any:
    decision = input("> Press enter to accept this option, or type chosen index. To ignore this variable, type i.")
    # select first (default) option
    if decision == "":
        chosen_index = new_indexes[0]
    # ignore variable
    elif decision.lower() == "i":
        chosen_index = -1
    # not valid input
    elif not decision.isdigit():
        chosen_index = None
    # select other item in list
    elif int(decision) in new_indexes:
        chosen_index = int(decision)
    else:
        chosen_index = None

    if chosen_index is None:
        print(f"Invalid option: It should be one in {new_indexes}.")

    return chosen_index


if __name__ == "__main__":
    main_cli()
