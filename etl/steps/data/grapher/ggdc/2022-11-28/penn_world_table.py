from owid import catalog

from etl.helpers import PathFinder

N = PathFinder(__file__)
VARS_TO_KEEP = [
    "rgdpe_pc",
    "rgdpo_pc",
    "cgdpe_pc",
    "cgdpo_pc",
    "rgdpna_pc",
    "rgdpe",
    "rgdpo",
    "cgdpe",
    "cgdpo",
    "rgdpna",
    "avh",
    "emp",
    "productivity",
    "labsh",
    "csh_c",
    "csh_i",
    "csh_g",
    "csh_x",
    "csh_m",
    "csh_r",
    "ccon",
    "cda",
    "cn",
    "rconna",
    "rdana",
    "rnna",
    "irr",
    "delta",
    "pop",
    "trade_openness",
    "rtfpna",
]


def run(dest_dir: str) -> None:
    dataset = catalog.Dataset.create_empty(dest_dir, N.garden_dataset.metadata)

    table = N.garden_dataset["penn_world_table"]

    # %%
    # Select country, year and only those variables with metadata specified
    # in the metadata sheet.

    id_vars = ["country", "year"]

    var_list = id_vars + VARS_TO_KEEP

    table = table[table.columns.intersection(var_list)]

    # if you data is in long format, check gh.long_to_wide_tables
    dataset.add(table)

    dataset.save()
