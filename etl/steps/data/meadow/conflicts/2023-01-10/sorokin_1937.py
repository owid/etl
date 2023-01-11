from shared import run_pipeline

from etl.helpers import PathFinder

# naming conventions
paths = PathFinder(__file__)
# column rename
COLUMN_RENAME = {
    "ID": "bulk_id",  # difference with default
    "Conflict name": "conflict_name",
    "Conflict participants": "conflict_participants",
    "Type of conflict": "type_of_conflict",
    "Start year": "start_year",
    "End year": "end_year",
    "Continent": "continent",
    "Total explicit deaths": "total_deaths",
    "Explicit Deaths->Explicit Mil->Explicit Direct": "deaths_military_direct",
    "Explicit Deaths->Explicit Mil->Explicit Indirect": "deaths_military_indirect",
    "Explicit Deaths->Explicit Mil->I/D-Not Explicit": "deaths_military_unclear",
    "Explicit Deaths->Mil/Civ-Not Explicit->Explicit Direct": "deaths_unclear_direct",
    "Explicit Deaths->Mil/Civ-Not Explicit->Explicit Indirect": "deaths_unclear_indirect",
    "Explicit Deaths->Mil/Civ-Not Explicit->I/D Not Explicit": "deaths_unclear_unclear",  # difference with default
    "Explicit Deaths->Explicit Civ->Explicit Direct": "deaths_civilian_direct",
    "Explicit Deaths->Explicit Civ->Explicit Indirect": "deaths_civilian_indirect",
    "Explicit Deaths->Explicit Civ->I/D-Not Explicit": "deaths_civilian_unclear",
    "D/W-Not explicit->Explicit Mil->Explicit Direct": "casualties_military_direct",  # difference with default
    "D/W-Not explicit>Explicit Mil->Explicit Indirect": "casualties_military_indirect",  # difference with default
    "D/W-Not explicit->Explicit Mil->I/D-Not Explicit": "casualties_military_unclear",  # difference with default
    "D/W-Not explicit->Mil/Civ-Not Explicit->Explicit Direct": "casualties_unclear_direct",  # difference with default
    "D/W-Not explicit->Mil/Civ-Not Explicit->Explicit Indirect": "casualties_unclear_indirect",  # difference with default
    "D/W-Not explicit->Mil/Civ-Not Explicit->I/D Not Explicit": "casualties_unclear_unclear",  # difference with default
    "D/W-Not explicit->Explicit Civ->Explicit Direct": "casualties_civilian_direct",  # difference with default
    "D/W-Not explicit->Explicit Civ->Explicit Indirect": "casualties_civilian_indirect",  # difference with default
    "D/W-Not explicit->Explicit Civ->I/D-Not Explicit": "casualties_civilian_unclear",  # difference with default
    "Source full reference": "source_full_reference",
    "Source page number or URL": "source_page_number_or_url",
    "Notes, inc. key quote": "notes_inc_key_quote",
    "Upload image": "upload_image",
    "Update": "update",
}


def run(dest_dir: str) -> None:
    run_pipeline(dest_dir, paths, COLUMN_RENAME)
