"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("farmed_finfishes_used_for_food.zip")

    #
    # Process data.
    #
    # The data is contained in text-extractable pdf files inside a zip folder.
    # The extraction cannot be done fully automatically. These are the steps I followed:
    # 1. Decompress the zip folder.
    # 2. Extract the text as follows:
    #    import PyPDF2
    #    pdf_path = "..."  # Path to one of the pdf files.
    #    def extract_pdf_text(pdf_path):
    #        text = ""
    #        with open(pdf_path, "rb") as file:
    #            pdf_reader = PyPDF2.PdfReader(file)
    #            for page_num in range(len(pdf_reader.pages)):
    #                page = pdf_reader.pages[page_num]
    #                text += page.extract_text()
    #        return text
    #    text=extract_pdf_text(pdf_path=pdf_path)
    # 3. Select the numeric part of the text, copy it into a text file, and do string replacements to the entire file (e.g. comma by nothing, double spaces by single spaces, single spaces by comma, etc.).
    # 4. Copy the resulting list of tuples here.

    # Content of Table S5: Estimated numbers of farmed fishes, & reported birds & mammals, killed for food 1990-2019.
    records = [
        (1990, 8676, 9, 18, 13, 100, 100, 31),
        (1991, 8981, 9, 18, 14, 104, 104, 32),
        (1992, 9908, 10, 20, 15, 114, 114, 34),
        (1993, 11213, 11, 23, 17, 129, 127, 35),
        (1994, 13051, 13, 26, 19, 150, 144, 37),
        (1995, 14995, 14, 29, 22, 173, 164, 39),
        (1996, 16872, 16, 33, 25, 194, 186, 40),
        (1997, 17895, 17, 36, 26, 206, 198, 42),
        (1998, 18507, 18, 37, 27, 213, 204, 43),
        (1999, 19875, 19, 40, 30, 229, 223, 45),
        (2000, 20813, 20, 42, 31, 240, 231, 47),
        (2001, 22165, 21, 45, 33, 255, 247, 48),
        (2002, 23516, 23, 48, 35, 271, 266, 50),
        (2003, 24230, 28, 59, 43, 279, 324, 51),
        (2004, 26307, 30, 63, 47, 303, 349, 52),
        (2005, 27980, 33, 70, 52, 322, 386, 54),
        (2006, 29801, 36, 76, 56, 343, 419, 55),
        (2007, 31605, 39, 83, 61, 364, 456, 58),
        (2008, 34281, 42, 89, 66, 395, 492, 61),
        (2009, 35754, 44, 95, 70, 412, 521, 63),
        (2010, 37745, 49, 103, 76, 435, 567, 64),
        (2011, 39448, 51, 110, 80, 455, 602, 66),
        (2012, 42336, 56, 122, 89, 488, 666, 67),
        (2013, 44988, 61, 132, 97, 519, 724, 68),
        (2014, 47215, 64, 140, 102, 544, 765, 70),
        (2015, 48979, 67, 147, 107, 565, 802, 72),
        (2016, 51043, 72, 158, 115, 588, 861, 74),
        (2017, 52631, 77, 166, 121, 607, 910, 76),
        (2018, 54431, 76, 167, 121, 627, 908, 79),
        (2019, 56327, 78, 171, 124, 649, 932, 80),
    ]
    tb = snap.read_from_records(
        records,
        columns=[
            "year",
            "production_kilotonnes",
            "n_fish_lower_billions",
            "n_fish_upper_billions",
            "n_fish_midpoint_billions",
            "production_relative_to_1990_pct",
            "n_fish_relative_to_1990_pct",
            "n_birds_and_mammals_billions",
        ],
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata, check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_meadow.save()
