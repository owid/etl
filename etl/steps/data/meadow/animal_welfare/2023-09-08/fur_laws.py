"""Load a snapshot and create a meadow dataset.

The data is manually extracted from the PDF and written in this script.
"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("fur_laws.pdf")

    # Manually extract data from the PDF file.
    columns = [
        "COUNTRY",
        "EU MS",
        "Fur farming ban/ phase-out due to stricter regulations",
        "Fur trade ban",
        "Starting date effective ban",
        "Operating fur farms",
    ]
    # There were other columns that I disregarded.
    # "Legislation ban",
    # "Stricter regulations on fur farming",
    # "Compensation/supporting measures",
    # "Fur legislation/General legislation",
    # "Original text",
    # "Translation",
    # "Extra info"]
    # NOTE: Some countries have multiple rows. I created a new row with the combined relevant information.
    data = [
        ("Austria", "YES", "YES", "", "2005", "NO"),
        ("Belarus", "NO", "NO", "", "", "YES"),
        ("Belgium", "YES", "YES", "", "2023", "NO"),
        ("Bosnia and Herzegovina", "NO", "YES", "", "2028", "YES"),
        # ("Bulgaria", "YES", "NO", "", "", "YES"),
        # ("Bulgaria", "YES", "", "", "", "YES"),
        # Combined:
        ("Bulgaria", "YES", "NO", "", "", "YES"),
        # ("Canada", "NO", "", "", "", "YES"),
        # ("Canada", "", "PARTIAL, phase-out of mink farming in the province of British Columbia", , "April, 2023", "YES"),
        # Combined:
        ("Canada", "NO", "PARTIAL", "", "2023", "YES"),
        # ("China", "NO", "NO", "", "", "YES"),
        # ("China", "NO", , , , "YES"),
        # Combined:
        ("China", "NO", "NO", "", "", "YES"),
        ("Croatia", "YES", "YES", "", "2017", "NO"),
        ("Cyprus", "YES", "NO", "", "", "YES"),
        ("Czech Republic", "YES", "YES", "", "2019", "NO"),
        # ("Denmark", "YES", "PARTIAL: ban on fox farming", "", "", "YES"),
        # ("Denmark", "YES", "PARTIAL: phase-out of raccoon dog farming", "", "", "YES"),
        # Combined:
        ("Denmark", "YES", "PARTIAL", "", "", "YES"),
        ("Estonia", "YES", "YES", "", "2026", "NO"),
        ("Finland", "YES", "NO", "", "", "YES"),
        ("France", "YES", "PARTIAL: non-domestic species ban", "", "November 2021", "NO"),
        ("Germany", "YES", "YES: phased-out due to stricter regulations", "", "2022", "NO"),
        # ("Greece", "YES", "NO", "", "", "YES"),
        # ("Greece", "YES", "NO", "", "", "YES"),
        # Combined:
        ("Greece", "YES", "NO", "", "", "YES"),
        ("Hungary", "YES", "PARTIAL", "", "", "YES"),
        ("Iceland", "NO", "NO", "", "", "YES"),
        ("Ireland", "YES", "YES", "", "4 April 2022", "NO"),
        ("Israel", "NO", "", "YES", "2021", ""),
        ("Italy", "YES", "YES", "", "July 2022", "NO"),
        ("Japan", "NO", "PARTIAL", "", "", "NO"),
        ("Latvia", "YES", "YES", "", "2028", "YES"),
        ("Lithuania", "YES", "Parliamentary debate", "", "", "YES"),
        ("Luxembourg", "YES", "YES", "", "", "NO"),
        ("Malta", "YES", "", "", "", "NO"),
        ("Montenegro", "NO", "", "", "", ""),
        ("Netherlands", "YES", "YES", "", "2020", "NO"),
        ("New Zealand", "NO", "", "PARTIAL", "2013", ""),
        ("North Macedonia", "NO", "YES", "", "2014", "NO"),
        ("Norway", "NO", "YES", "", "2025", "YES"),
        ("Poland", "YES", "Parliamentary debate", "", "", "YES"),
        ("Portugal", "YES", "NO", "", "", "NO"),
        ("Romania", "YES", "Parliamentary debate", "", "", "YES"),
        ("Russia", "NO", "NO", "", "", "YES"),
        ("Serbia", "NO", "YES", "", "2019", "NO"),
        ("Slovakia", "YES", "YES", "", "2025", "YES"),
        ("Slovenia", "YES", "YES", "", "2013", "NO"),
        ("Spain", "YES", "PARTIAL & Parliamentary debate", "", "", "YES"),
        ("Sweden", "YES", "PARTIAL", "", "", "YES"),
        ("Switzerland", "NO", "YES", "", "", "NO"),
        ("Ukraine", "NO", "", "", "", "YES"),
        ("United Kingdom", "NO", "YES", "", "2000", "NO"),
        # ("United States", "NO", "NO", "", "", "YES"),
        # ("United States", "NO", "Parliamentary debate", "YES, fur sales bans in Lexington, MA (2923), Cambridge, MA (2022), Plymouth, MA (2022), Brookline, MA (2021), Hallandale Beach, FL (2021), Boulder, CO (2021), Ann Arbor, MI (2021), Weston, MA (2021), Wellesley, MA (2020), State of California (2019), Los Angeles, CA (2018), San Francisco, CA (2018), Berkely, CA (2018), West Hollywood, CA (2011)", "", "YES"),
        # Combined:
        ("United States", "NO", "Parliamentary debate", "PARTIAL", "", "YES"),
    ]
    tb = snap.read_from_records(columns=columns, data=data)

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
