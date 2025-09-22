"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gender_statistics")

    # Read table from meadow dataset.
    tb = ds_meadow.read("gender_statistics")

    # Retrieve snapshot.
    snap = paths.load_snapshot("gender_statistics.zip")
    # Load metadata from snapshot.
    tb_meta = snap.read_in_archive("Gender_StatsSeries.csv", low_memory=False, safe_types=False)
    tb_meta = tb_meta[
        [
            "Series Code",
            "Indicator Name",
            "Long definition",
            "Unit of measure",
            "Aggregation method",
            "Limitations and exceptions",
            "Notes from original source",
            "General comments",
            "Source",
        ]
    ]
    tb_meta = tb_meta.rename(columns={"Series Code": "indicator_code"})

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Pivot table to have indicators as columns
    tb = tb.pivot(index=["country", "year"], columns="indicator_code", values="value")
    tb = tb.reset_index()

    # Add metadata to indicators
    tb = add_metadata(tb, tb_meta)

    # Improve table format.
    tb = tb.format(["country", "year"])
    # Combine maternity and paternity leave indicators (time only available TO each parent)
    tb["total_maternity_leave_to"] = tb["sh_mmr_leve"] + tb["sh_par_leve_fe"]
    tb["total_paternity_leave_to"] = tb["sh_par_leve_ma"] + tb["sh_ptr_leve"]

    # Combine maternity and paternity leave indicators (time available days FOR each parent)
    tb["total_maternity_leave_for"] = tb["sh_mmr_leve"] + tb["sh_par_leve_fe"] + tb["sh_par_leve"]
    tb["total_paternity_leave_for"] = tb["sh_par_leve_ma"] + tb["sh_ptr_leve"] + tb["sh_par_leve"]

    # Add relevant metadata to the newly created columns
    add_metadata_description(tb, "total_maternity_leave_for", ["sh_mmr_leve", "sh_par_leve_fe", "sh_par_leve"])
    add_metadata_description(tb, "total_paternity_leave_for", ["sh_par_leve_ma", "sh_ptr_leve", "sh_par_leve"])

    add_metadata_description(tb, "total_maternity_leave_to", ["sh_mmr_leve", "sh_par_leve_fe"])
    add_metadata_description(tb, "total_paternity_leave_to", ["sh_par_leve_ma", "sh_ptr_leve"])
    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def add_metadata_description(tb: Table, column_name: str, indicators: list) -> None:
    """Adds metadata description to a given column in tb."""
    description = (
        f"**This indicator is a sum of {len(indicators)} different leave indicators provided by World Bank:**\n\n"
    )
    for indicator in indicators:
        if indicator in tb and hasattr(tb[indicator], "metadata"):
            description += (
                f"The indicator '{tb[indicator].metadata.title}' is described by World Bank as:\n\n"
                f"{tb[indicator].metadata.description_from_producer}\n\n"
            )
    tb[column_name].metadata.description_from_producer = description


def add_metadata(tb: Table, tb_meta: pd.DataFrame) -> Table:
    """Add metadata to table columns using World Bank metadata.

    Args:
        tb: Table with data columns to add metadata to
        tb_meta: DataFrame with metadata information

    Returns:
        Table with updated metadata
    """
    # Create a mapping from indicator_code to metadata
    metadata_dict = tb_meta.set_index("indicator_code").to_dict("index")

    for column in tb.columns:
        if column not in ["country", "year"]:
            # Get the column metadata object
            meta = tb[column].metadata

            if column in metadata_dict:
                meta_info = metadata_dict[column]

                # Set title from Indicator Name
                indicator_name = meta_info.get("Indicator Name", column)
                meta.title = indicator_name

                # Set source attribution
                source = meta_info.get("Source", "")
                if source and pd.notna(source):
                    cleaned_source = clean_source_field(source)
                    if cleaned_source:
                        meta.presentation = {"attribution": cleaned_source}

                # Handle units
                unit_of_measure = meta_info.get("Unit of measure", "")
                if pd.notna(unit_of_measure) and str(unit_of_measure).strip():
                    unit_lower = str(unit_of_measure).lower()
                    if "percent" in unit_lower or "%" in unit_lower:
                        meta.unit = "%"
                        meta.short_unit = "%"
                    else:
                        meta.unit = unit_of_measure
                        meta.short_unit = ""
                else:
                    # Set default unit if missing or NaN
                    meta.unit = ""
                    meta.short_unit = ""

                # Build description from producer
                description_parts = []

                # Add long definition
                long_def = meta_info.get("Long definition", "")
                if pd.notna(long_def) and long_def:
                    description_parts.append(f"**Definition:** {long_def}")

                # Add aggregation method
                agg_method = meta_info.get("Aggregation method", "")
                if pd.notna(agg_method) and agg_method:
                    description_parts.append(f"**Aggregation method:** {agg_method}")

                # Add limitations and exceptions
                limitations = meta_info.get("Limitations and exceptions", "")
                if pd.notna(limitations) and limitations:
                    description_parts.append(f"**Limitations and exceptions:** {limitations}")

                # Add notes from original source
                notes = meta_info.get("Notes from original source", "")
                if pd.notna(notes) and notes:
                    description_parts.append(f"**Notes from original source:** {notes}")

                # Add general comments
                comments = meta_info.get("General comments", "")
                if pd.notna(comments) and comments:
                    description_parts.append(f"**General comments:** {comments}")

                # Combine all parts
                if description_parts:
                    meta.description_from_producer = "\n\n".join(description_parts)
                else:
                    meta.description_from_producer = f"World Bank indicator: {column}"
            else:
                # Column not found in metadata - set defaults
                meta.title = column
                meta.unit = ""
                meta.short_unit = ""
                meta.description_from_producer = f"World Bank indicator: {column}"

    return tb


def clean_source_field(source: str) -> str:
    """Clean up source field by simplifying to key organization names."""
    if not source or pd.isna(source):
        return ""

    import re

    # Create a simple lookup table for common sources to clean names
    source_mappings = {
        # World Bank variants
        "world bank": "World Bank",
        "wb": "World Bank",
        "world bank group": "World Bank",
        "wbg": "World Bank",
        "doing business": "World Bank",
        "enterprise surveys": "World Bank",
        "women, business and the law": "Women, Business and the Law",
        "entrepreneurship survey": "World Bank",
        "global findex": "World Bank",
        # UN variants
        "united nations population division": "UN Population Division via World Bank",
        "world population prospects": "UN Population Division via World Bank",
        "world urbanization prospects": "UN Population Division via World Bank",
        "world marriage data": "UN Population Division via World Bank",
        "trends in international migrant stock": "UN Population Division via World Bank",
        "united nations": "UN via World Bank",
        # UNESCO variants
        "unesco institute for statistics": "UNESCO via World Bank",
        "uis": "UNESCO via World Bank",
        "stat bulk data download service": "UNESCO via World Bank",
        # WHO variants
        "world health organization": "WHO via World Bank",
        "global health observatory": "WHO via World Bank",
        "global health estimates": "WHO via World Bank",
        "maternal mortality": "WHO via World Bank",
        "trends in maternal mortality": "WHO via World Bank",
        # UNICEF variants
        "un children's fund": "UNICEF via World Bank",
        "unicef data": "UNICEF via World Bank",
        "state of the world's children": "UNICEF via World Bank",
        "childinfo": "UNICEF via World Bank",
        # ILO variants
        "international labour organization": "ILO via World Bank",
        "ilostat": "ILO via World Bank",
        "labour force statistics": "ILO via World Bank",
        "ilo modelled estimates": "ILO via World Bank",
        # Other UN agencies
        "international monetary fund": "IMF via World Bank",
        "joint united nations programme on hiv/aids": "UNAIDS via World Bank",
        "international telecommunication union": "ITU via World Bank",
        "inter-parliamentary union": "IPU via World Bank",
        "un women": "UN Women via World Bank",
        "unfpa": "UNFPA via World Bank",
        "unodc": "UNODC via World Bank",
        # Survey programs
        "demographic and health surveys": "DHS via World Bank",
        "dhs program": "DHS via World Bank",
        "multiple indicator cluster surveys": "MICS via World Bank",
        # Other organizations
        "organisation for economic co-operation and development": "OECD via World Bank",
        "organization for economic co-operation and development": "OECD via World Bank",
        "eurostat": "Eurostat via World Bank",
        "human mortality database": "Human Mortality Database via World Bank",
        "luxembourg income study": "Luxembourg Income Study via World Bank",
    }

    # Convert to lowercase for matching
    source_lower = source.lower()

    # Special handling for World Bank methodology citations
    if "world bank" in source_lower and ("methodology" in source_lower or "based on" in source_lower):
        # Extract author and year from citations like "Filmer et al. (2018)"
        author_year_match = re.search(r"([A-Za-z]+(?:\s+et\s+al\.)?)\s+\((\d{4})\)", source)
        if author_year_match:
            author = author_year_match.group(1)
            year = author_year_match.group(2)
            return f"World Bank based on methodology in {author} ({year})"

    # Find matching organizations
    found_orgs = set()
    for key, org_name in source_mappings.items():
        if key in source_lower:
            found_orgs.add(org_name)

    # Handle joint initiatives specifically
    if "joint" in source_lower and ("unicef" in source_lower or "who" in source_lower or "world bank" in source_lower):
        if "malnutrition" in source_lower or "jme" in source_lower:
            found_orgs = {"UNICEF-WHO-World Bank JME"}
        elif "child mortality" in source_lower:
            found_orgs = {"UN IGME"}
        elif "water" in source_lower and "sanitation" in source_lower:
            found_orgs = {"WHO-UNICEF JMP"}

    # Special cases for household surveys
    if "household surveys" in source_lower:
        found_orgs.add("Household surveys")

    if "national statistical offices" in source_lower or "country official statistics" in source_lower:
        found_orgs.add("National statistical offices")

    # If no specific organizations found, try to extract the first meaningful part
    if not found_orgs:
        # Remove all URLs and technical metadata first
        clean_text = re.sub(r"https?://[^\s,;)\]]+", "", source)
        clean_text = re.sub(r"\([^)]*\)", "", clean_text)
        clean_text = re.sub(r"uri:.*?(?=[,;]|$)", "", clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r"note:.*?(?=[,;]|$)", "", clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r"publisher:.*?(?=[,;]|$)", "", clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r"type:.*?(?=[,;]|$)", "", clean_text, flags=re.IGNORECASE)
        clean_text = re.sub(r"data accessed:.*?(?=[,;]|$)", "", clean_text, flags=re.IGNORECASE)

        # Take the first substantial part
        parts = re.split(r"[;,\n]", clean_text)
        for part in parts:
            part = part.strip()
            if len(part) > 10 and not re.match(r"^\s*(staff|estimates|based|data|compiled)", part, re.IGNORECASE):
                found_orgs.add(part[:50])  # Truncate if too long
                break

    if not found_orgs:
        return ""

    # Return up to 2 organizations, sorted for consistency
    result_orgs = sorted(list(found_orgs))[:2]
    return "; ".join(result_orgs)
