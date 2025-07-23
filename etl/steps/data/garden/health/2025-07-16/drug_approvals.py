"""Load a meadow dataset and create a garden dataset."""

from datetime import datetime

from matplotlib import pyplot as plt
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def orange_book_approval_year(approval_date):
    if approval_date == "Approved Prior to Jan 1, 1982":
        return None
    else:
        return datetime.strptime(approval_date, "%b %d, %Y").year


def cder_duplicates(appl_no, cder_unique_ls, cder_ob_ls, cd_pb_ls):
    if appl_no in cder_ob_ls:
        return "Orange Book Duplicate"
    if appl_no in cd_pb_ls:
        return "Purple Book Duplicate"
    if appl_no not in cder_ob_ls and appl_no not in cd_pb_ls:
        cder_unique_ls.append(appl_no)
        return "Unique"


def orange_book_duplicates(appl_no, cder_ob_ls):
    if appl_no in cder_ob_ls:
        return "CDER Duplicate"
    else:
        return "Unique"


def purple_book_duplicates(appl_no, cd_pb_ls):
    if appl_no in cd_pb_ls:
        return "CDER Duplicate"
    else:
        return "Unique"


def get_approvals_per_year(tb_cder, tb_orange_book, tb_purple_book, cder_unique_ls, cder_ob_ls, cd_pb_ls):
    # make copy of tables to avoid modifying original data
    tb_cder = tb_cder.copy()
    tb_orange_book = tb_orange_book.copy()
    tb_purple_book = tb_purple_book.copy()

    tb_cder["source"] = "cder"
    tb_cder["duplicate"] = tb_cder["application_number"].apply(
        lambda appl_no: cder_duplicates(appl_no, cder_unique_ls, cder_ob_ls, cd_pb_ls)
    )
    tb_cder["info"] = ""

    tb_orange_book["approval_year"] = tb_orange_book["approval_date"].apply(orange_book_approval_year)
    tb_orange_book["source"] = "orange_book"
    tb_orange_book["duplicate"] = tb_orange_book["application_number"].apply(
        lambda appl_no: orange_book_duplicates(appl_no, cder_ob_ls)
    )
    tb_orange_book["info"] = tb_orange_book["application_type"]

    tb_purple_book["approval_year"] = tb_purple_book["approval_date"].apply(lambda x: x.year if x else None)
    tb_purple_book["source"] = "purple_book"
    tb_purple_book["duplicate"] = tb_purple_book["application_number"].apply(
        lambda appl_no: purple_book_duplicates(appl_no, cd_pb_ls)
    )
    tb_purple_book["info"] = tb_purple_book["center"]

    tb = pr.concat(
        [
            tb_cder[["approval_year", "source", "application_type", "duplicate", "info"]],
            tb_orange_book[["approval_year", "source", "application_type", "duplicate", "info"]],
            tb_purple_book[["approval_year", "source", "application_type", "duplicate", "info"]],
        ],
        ignore_index=True,
    )

    tb = (
        tb.groupby(["approval_year", "source", "application_type", "duplicate", "info"])
        .size()
        .reset_index(name="approvals")
    )
    tb["approval_year"] = tb["approval_year"].astype(int)
    return tb


def plot_per_source(tb, source, by="application_type", color=None, title=None):
    tb_source = tb[tb["source"] == source][["approval_year", "approvals", by]].copy()
    tb_source = tb_source.groupby(["approval_year", by]).sum().reset_index()

    tb_pv = tb_source.pivot(index="approval_year", columns=by, values="approvals")

    if color:
        ordered_series = list(color.keys())
        tb_pv = tb_pv[ordered_series]

    tb_pv.plot(kind="bar", figsize=(8, 5), stacked=True, color=color)
    plt.xlabel("Approval Year")
    plt.ylabel("Approvals")
    if title:
        plt.title(f"{title}")
    else:
        plt.title(f"Drug Approvals per Year ({source})")
    plt.legend(title=by)
    plt.tight_layout()
    plt.show()


def check_duplicates(tb_cder, tb_orange_book, tb_purple_book, verbose=True):
    appl_no_cder = tb_cder["application_number"].unique()
    appl_no_orange_book = tb_orange_book["application_number"].unique()
    appl_no_purple_book = tb_purple_book["application_number"].unique()

    # check for duplicates in cder
    cder_unique_ls = []
    cder_ob_ls = []
    cd_pb_ls = []
    for appl_no in appl_no_cder:
        if appl_no in appl_no_orange_book:
            cder_ob_ls.append(appl_no)
            if verbose:
                name_ob = tb_orange_book[tb_orange_book["application_number"] == appl_no]["proprietary_name"].values[0]
                name_cder = tb_cder[tb_cder["application_number"] == appl_no]["proprietary_name"].values[0]
                print(f"Duplicate found in cder and orange book for application number: {appl_no}")
                print(f"Orange Book Trade Name: {name_ob}, Cder Proprietary Name: {name_cder}")
        if appl_no in appl_no_purple_book:
            cd_pb_ls.append(appl_no)
            if verbose:
                name_pb = tb_purple_book[tb_purple_book["application_number"] == appl_no]["proprietary_name"].values[0]
                name_cder = tb_cder[tb_cder["application_number"] == appl_no]["proprietary_name"].values[0]
                print(f"Duplicate found in cder and purple book for application number: {appl_no}")
                print(f"Purple Book Trade Name: {name_pb}, Cder Proprietary Name: {name_cder}")
        if appl_no not in appl_no_orange_book and appl_no not in appl_no_purple_book:
            cder_unique_ls.append(appl_no)
            if verbose:
                print(tb_cder[tb_cder["application_number"] == appl_no])

    print(f"cder unique: {len(cder_unique_ls)}")
    print(f"cder orange book: {len(cder_ob_ls)}")
    print(f"cder purple book: {len(cd_pb_ls)}")

    # check for duplicates between orange and purple book (there shouldn't be any):
    ob_pb_duplicates = []
    for appl_no in appl_no_orange_book:
        if appl_no in appl_no_purple_book:
            print(f"Duplicate found in orange and purple book for application number: {appl_no}")
            print(tb_orange_book[tb_orange_book["application_number"] == appl_no])
            print(tb_purple_book[tb_purple_book["application_number"] == appl_no])
            ob_pb_duplicates.append(appl_no)
    assert len(ob_pb_duplicates) == 0, "There should be no duplicates between orange book and purple book."

    return cder_unique_ls, cder_ob_ls, cd_pb_ls


def run() -> None:
    # Load inputs.
    #
    # Load meadow dataset.
    ds_cder = paths.load_dataset("cder_approvals")
    # ds_drugs_fda = paths.load_dataset("drugs_approvals")
    ds_orange_book = paths.load_dataset("orange_book")
    ds_purple_book = paths.load_dataset("purple_book")

    # Read table from meadow dataset.
    tb_cder = ds_cder.read("cder_approvals")
    # tb_drugs_fda_sub = ds_drugs_fda.read("drugs_fda_submissions")
    # tb_drugs_fda_products = ds_drugs_fda.read("drugs_fda_products")
    # tb_open_fda = ds_drugs_fda.read("drugs_open_fda")
    tb_orange_book = ds_orange_book.read("orange_book")
    tb_purple_book = ds_purple_book.read("purple_book")

    # Normalize relevant column names.
    tb_cder = tb_cder.rename(
        columns={
            "application_number__1": "application_number",
            "proprietary__name": "proprietary_name",
            "approval_date": "approval_date",
            "nda_bla": "application_type",
        }
    )
    tb_orange_book = tb_orange_book.rename(
        columns={
            "appl_no": "application_number",
            "trade_name": "proprietary_name",
            "approval_date": "approval_date",
            "appl_type": "application_type",
        }
    )
    tb_purple_book = tb_purple_book.rename(
        columns={
            "bla_number": "application_number",
            "proprietary_name": "proprietary_name",
            "approval_date": "approval_date",
            "bla_type": "application_type",
        }
    )

    # get duplicates
    cder_unique_ls, cder_ob_ls, cd_pb_ls = check_duplicates(tb_cder, tb_orange_book, tb_purple_book, verbose=False)

    # get approvals per year
    tb = get_approvals_per_year(tb_cder, tb_orange_book, tb_purple_book, cder_unique_ls, cder_ob_ls, cd_pb_ls)

    # Plot approvals per year for each source.
    # standard tab 20 coloring:
    # blue = "#1f77b4"
    # orange = "#ff7f0e"
    # green = "#2ca02c"
    # red = "#d62728"
    color_map_cder = {"BLA": "#1f77b4", "NDA": "#ff7f0e"}
    color_map_ob = {"N": "#1f77b4", "A": "#ff7f0e"}
    color_map_pb = {"351(a)": "#1f77b4", "351(k) Biosimilar": "#ff7f0e", "351(k) Interchangeable": "#2ca02c"}

    color_map_cder_2 = {"Orange Book Duplicate": "#EDA600", "Purple Book Duplicate": "#8300BF", "Unique": "#4BC8E1"}
    color_map_dup = {"CDER Duplicate": "#EDA600", "Unique": "#4BC8E1"}

    plot_per_source(tb, "cder", color=color_map_cder)
    plot_per_source(tb, "orange_book", color=color_map_ob)
    plot_per_source(tb, "purple_book", color=color_map_pb)

    plot_per_source(tb, "cder", by="duplicate", color=color_map_cder_2)
    plot_per_source(tb, "orange_book", by="duplicate", color=color_map_dup)
    plot_per_source(tb, "purple_book", by="duplicate", color=color_map_dup)

    # now only relevant duplicates
    plot_per_source(
        tb[tb["info"] == "N"],
        "orange_book",
        by="duplicate",
        color=color_map_dup,
        title="Orange Book duplicates (only NDAs)",
    )
    plot_per_source(
        tb[tb["info"] == "CDER"],
        "purple_book",
        by="duplicate",
        color=color_map_dup,
        title="Purple Book duplicates (only CDER approvals)",
    )
    plot_per_source(
        tb[tb["info"] == "CBER"],
        "purple_book",
        by="duplicate",
        color=color_map_dup,
        title="Purple Book duplicates (only CBER approvals)",
    )

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save garden dataset.
    ds_garden.save()
