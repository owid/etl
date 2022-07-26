"""Grapher step for the fossil fuel production dataset.

"""

from copy import deepcopy
from typing import cast

from etl import grapher_helpers as gh
from owid import catalog
from owid.catalog.utils import underscore


def adapt_dataset_metadata_for_grapher(
    metadata: catalog.DatasetMeta,
) -> catalog.DatasetMeta:
    """Adapt metadata of a garden dataset to be used in a grapher step.

    Parameters
    ----------
    metadata : catalog.DatasetMeta
        Dataset metadata.

    Returns
    -------
    metadata : catalog.DatasetMeta
        Adapted dataset metadata, ready to be inserted into grapher.

    """
    metadata = deepcopy(metadata)

    assert (
        len(metadata.sources) >= 1
    ), "Dataset needs to have at least one source in metadata."

    # Define the 'default_source', which will be the one where all sources' attributes are combined.
    default_source = metadata.sources[0]
    # Attributes to combine from sources.
    attributes = [
        "name",
        "description",
        "url",
        "source_data_url",
        "owid_data_url",
        "date_accessed",
        "publication_date",
        "publication_year",
        "published_by",
        "publisher_source",
    ]
    # Combine sources' attributes into the first source (which is the only one that grapher will interpret).
    for attribute in attributes:
        # Gather non-empty values from each source for current attribute.
        values = list(
            set(
                [
                    getattr(source, attribute)
                    for source in metadata.sources
                    if getattr(source, attribute) is not None
                ]
            )
        )
        if attribute == "description":
            if metadata.description is not None:
                # Add the dataset description as if it was a source's description.
                values = [metadata.description] + values

            # Descriptions are usually long, so it is better so put together descriptions from different sources in
            # separate lines.
            combined_value = "\n".join(values)
        elif attribute == "date_accessed":
            # For dates simply take the one from the first source.
            combined_value = values[0]
        else:
            # For any other attribute, values from different sources can be in the same line, separated by ;.
            combined_value = " ; ".join(values)

        setattr(default_source, attribute, combined_value)

    # Remove other sources and keep only the default one.
    metadata.sources = [default_source]

    # Add institution and year to dataset short name (the name that will be used in grapher database).
    short_name_ending = "__" + underscore(f"{metadata.namespace}_{metadata.version}")
    if not metadata.short_name.endswith(short_name_ending):
        metadata.short_name = metadata.short_name + short_name_ending
    # Empty dataset description (otherwise it will appear in `Internal notes` in the admin UI).
    metadata.description = ""

    return metadata


def adapt_table_for_grapher(
    table: catalog.Table, country_col: str = "country", year_col: str = "year"
) -> catalog.Table:
    """Adapt table (from a garden dataset) to be used in a grapher step.

    Parameters
    ----------
    table : catalog.Table
        Table from garden dataset.
    country_col : str
        Name of country column in table.
    year_col : str
        Name of year column in table.

    Returns
    -------
    table : catalog.Table
        Adapted table, ready to be inserted into grapher.

    """
    table = deepcopy(table)
    # Grapher needs a column entity id, that is constructed based on the unique entity names in the database.
    table["entity_id"] = gh.country_to_entity_id(
        table[country_col], create_entities=True
    )
    table = table.drop(columns=[country_col]).rename(columns={year_col: "year"})
    table = table.set_index(["entity_id", "year"])

    return cast(catalog.Table, table)
