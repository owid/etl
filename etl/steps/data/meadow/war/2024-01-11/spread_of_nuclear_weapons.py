"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("spread_of_nuclear_weapons.csv")

    #
    # Process data.
    #
    # The raw data is in a table in a PDF document, which is hard to parse programmatically.
    # Instead, we manually extract the values here.
    columns = ["Country", "Explore", "Pursue", "Acquire"]
    data = [
        ("United States", "1939-", "1942-", "1945-"),
        ("Russia", "1942-", "1943-", "1949-"),
        ("United Kingdom", "1940-", "1941-", "1952-"),
        ("France", "1945-", "1954-", "1960-"),
        ("China", "1952-", "1955-", "1964-"),
        ("Israel", "1949-", "1955-", "1967-"),
        ("South Africa", "1969-91", "1974-91", "1979-91"),
        ("Pakistan", "1972-", "1972-", "1987-"),
        ("India", "1948-", "1964-66,72-75,80-", "1987-"),
        ("Korea, North", "1962-", "1980-", "2006-"),
        ("Yugoslavia", "1949-62,74-87", "1953-62,82-87", ""),
        ("South Korea", "1969-81", "1970-81", ""),
        ("Libya", "1970-2003", "1970-2003", ""),
        ("Brazil", "1966-90", "1975-90", ""),
        ("Iraq", "1975-91", "1981-91", ""),
        ("Iran", "1974-79,84-", "1989-", ""),
        ("Syria", "2000-", "2002-07", ""),
        ("Germany", "1939-45", "", ""),
        ("Japan", "1941-45,67-72", "", ""),
        ("Switzerland", "1945-69", "", ""),
        ("Sweden", "1945-70", "", ""),
        ("Norway", "1947-62", "", ""),
        ("Egypt", "1955-80", "", ""),
        ("Italy", "1955-58", "", ""),
        ("Australia", "1956-73", "", ""),
        ("Germany, West", "1957-58", "", ""),
        ("Indonesia", "1964-67", "", ""),
        ("Taiwan", "1967-76,87-88", "", ""),
        ("Romania", "1978-89", "", ""),
        ("Argentina", "1978-90", "", ""),
        ("Algeria", "1983-91", "", ""),
    ]
    tb = snap.read_from_records(columns=columns, data=data)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()
