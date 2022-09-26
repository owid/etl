from copy import deepcopy
from pathlib import Path
from typing import List

from owid.catalog import Dataset, Source, Table, Variable

from etl.paths import DATA_DIR

CURRENT_DIR = Path(__file__).parent
METADATA_PATH = CURRENT_DIR / "internet.meta.yml"


def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")

    # Create and add users table
    table_users = make_users_table()
    ds.add(table_users)
    # Add sources
    ds.metadata.sources = combine_all_variable_sources(ds)
    # Save
    ds.save()


def make_users_table() -> Table:
    # Load internet data
    d = Dataset(DATA_DIR / "garden/worldbank_wdi/2022-05-26/wdi")
    table_internet = d["wdi"].dropna(subset=["it_net_user_zs"])[["it_net_user_zs"]]
    # Load population data
    d = Dataset(DATA_DIR / "garden/owid/latest/key_indicators")
    table_population = d["population"]
    # Merge
    table = table_population.merge(table_internet, left_index=True, right_index=True)
    # Estimate number of internet users
    num_internet_users = (table.population * table.it_net_user_zs / 100).round().astype(int)
    # Add to table
    var_name = "num_internet_users"
    table = table.assign(
        **{
            var_name: Variable(num_internet_users, name=var_name),
            "share_internet_users": table.it_net_user_zs,
        }
    )
    # Filter columns
    table = table[["num_internet_users", "share_internet_users"]]
    # Propagate metadata
    table.share_internet_users.metadata = deepcopy(table_internet.it_net_user_zs.metadata)
    table.num_internet_users.metadata = deepcopy(table_internet.it_net_user_zs.metadata)
    table.num_internet_users.metadata.sources += table_population.population.metadata.sources
    table.num_internet_users.metadata.description = (
        "The number of internet users is calculated by Our World in Data based on internet access figures "
        "as a share of the total population, published in the World Bank, World Development Indicators "
        "and total population figures from the UN World Population Prospects, Gapminder and HYDE.\n\n"
        + table.num_internet_users.metadata.description
    )
    # Metadata from YAML
    table.update_metadata_from_yaml(METADATA_PATH, "users")
    return table


def combine_all_variable_sources(ds: Dataset) -> List[Source]:
    sources = []
    for t in ds:
        # Collect sources from variables
        sources.extend([source for col in t.columns for source in t[col].metadata.sources])
    # Unique sources
    sources = [Source.from_dict(dict(ss)) for ss in set(frozenset(s.to_dict().items()) for s in sources)]
    return sources
