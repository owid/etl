#
#  open_numbers.py
#  etl
#

"""
Convert repositories with data in DDF format to OWID's Dataset and Table
format.
"""

import datetime as dt
import hashlib
import tempfile
import warnings
from multiprocessing import Pool
from pathlib import Path
from typing import Dict, List, Tuple, cast

import frictionless
import pandas as pd
import structlog
from frictionless.exception import FrictionlessException
from owid.catalog import Dataset, Table, Variable, utils
from owid.catalog.meta import Source
from owid.repack import repack_series

from etl.git import GithubRepo
from etl.paths import DATA_DIR

log = structlog.get_logger()


def run(dest_dir: str) -> None:
    # identify the short name and the repo
    short_name = dest_dir.split("/")[-1]
    repo_short_name = "ddf--" + short_name.replace("__", "--")
    repo = GithubRepo("open-numbers", repo_short_name)
    assert repo.cache_dir.is_dir()

    # load it as a frictionless package
    package = frictionless.Package((repo.cache_dir / "datapackage.json"))

    # make an empty dataset
    ds = Dataset.create_empty(dest_dir)

    # copy metadata from frictionless
    ds.metadata.namespace = "open_numbers"
    ds.metadata.short_name = short_name
    ds.metadata.title = package.title or None
    ds.metadata.sources = [Source(url=repo.github_url, date_accessed=str(dt.date.today()))]

    if package.description and package.title != package.description:
        ds.metadata.description = package.description
    ds.save()

    # name remapping
    resource_map = remap_names(package.resources)

    # copy tables one by one
    with Pool() as pool:
        args = [(ds, repo, short_name, resources) for short_name, resources in resource_map.items()]
        pool.starmap(add_resource, args)


def add_resource(
    ds: Dataset,
    repo: GithubRepo,
    short_name: str,
    resources: List[frictionless.Resource],
) -> None:
    print(f"- {short_name}")
    try:
        if len(resources) > 1:
            df = load_and_combine(repo.cache_dir, resources)
        else:
            df = load_table(resources[0])

    except FrictionlessException:
        # see: https://github.com/owid/etl/issues/36
        print("  ERROR: skipping")
        return

    t = Table(df)
    t.metadata.short_name = short_name

    # adapt the table name and its column names to our naming convention
    t = utils.underscore_table(t)

    # we've already repacked the data
    ds.add(t, repack=False)


def load_table(resource: frictionless.Resource) -> pd.DataFrame:
    df = cast(pd.DataFrame, resource.to_pandas())

    # use smaller, more accurate column types that minimise space
    if "global" in df.columns:
        df["geo"] = df.pop("global")

    primary_key = resource.schema.primary_key

    df.reset_index(inplace=True)

    # repack dataframe in place
    for col in df.columns:
        df[col] = repack_series(df[col])

    df.set_index(primary_key, inplace=True)

    return df


def load_and_combine(path: Path, resources: List[frictionless.Resource]) -> pd.DataFrame:
    first = True
    primary_key: List[str] = []
    columns: List[str]

    with tempfile.NamedTemporaryFile(suffix=".csv") as f:
        for resource in resources:
            if first:
                # print csv header
                primary_key = resource.schema.primary_key
                columns = [k.name for k in resource.schema.fields]

                if "global" in columns:
                    remap = {"global": "geo"}
                    columns = [remap.get(c, c) for c in columns]
                    primary_key = [remap.get(c, c) for c in primary_key]

                f.write(",".join(columns).encode("utf8") + b"\n")
                first = False

            with open((path / resource.path).as_posix(), "rb") as istream:
                lines = iter(istream)
                next(lines)  # skip the header
                for line in lines:
                    f.write(line)

        f.flush()

        # ignore mixed type warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", pd.errors.DtypeWarning)
            df = pd.read_csv(f.name)

    # fix mixed types of object columns
    for col in df.select_dtypes(object).columns:
        if pd.api.types.infer_dtype(df[col]).startswith("mixed"):
            # try numeric first and fall back to string
            try:
                df[col] = pd.to_numeric(df[col])
            except ValueError:
                df[col] = df[col].astype(str)

    # repack dataframe in place
    for col in df.columns:
        df[col] = repack_series(df[col])

    df.set_index(primary_key, inplace=True)

    return cast(pd.DataFrame, df)


def remap_names(
    resources: List[frictionless.Resource],
) -> Dict[str, frictionless.Resource]:
    "Short names must be unique, so fix name collisions."
    rows = []
    for resource in resources:
        # ignore categories for now
        if not resource.name.startswith("ddf--datapoints"):
            continue

        name, hashed_name = parse_name(resource.name)
        rows.append(
            {
                "name": name,
                "hashed_name": hashed_name,
                "primary_key": tuple(norm_primary_key(resource.schema.primary_key)),
                "resource": resource,
            }
        )

    if not rows:
        return {}

    df = pd.DataFrame.from_records(rows)

    names = {}
    for name, group in df.groupby("name"):
        if len(group.primary_key.unique()) == 1:
            names[name] = list(group.resource)
            continue

        # multiple primary keys found for the same name, give each a unique suffix
        for primary_key, subgroup in group.groupby("primary_key"):
            suffix = hashlib.md5(",".join(primary_key).encode("utf8")).hexdigest()[:4]
            names[f"{name}_{suffix}"] = list(subgroup.resource)

    return names


def parse_name(name: str) -> Tuple[str, str]:
    # ddf files have names like "ddf--datapoints--deaths--by--country--age--year.csv""""
    parts = name[len("ddf--datapoints--") : -4].split("--")
    assert parts[1] == "by"

    preferred_name = parts[0]
    # suffix is a content-based hash of the dimension names
    hash_suffix = hashlib.md5("__".join(parts[2:]).encode("utf8")).hexdigest()[:4]
    hashed_name = f"{preferred_name}_{hash_suffix}"

    return preferred_name, hashed_name


def norm_primary_key(primary_key: List[str]) -> List[str]:
    return [k if k != "global" else "geo" for k in primary_key]


GM_TO_OWID_ISO_CODES = {
    "KOS": "OWID_KOS",
    "NLD_CURACAO": "CUW",
}


def iso_gm2owid(ds: Variable) -> Variable:
    # Load reference OWID country file
    countries_regions = Dataset(DATA_DIR / "garden/regions/2023-01-01/regions")["regions"]
    # Standardize
    return ds.str.upper().map(GM_TO_OWID_ISO_CODES).map(countries_regions["name"])


def fast_table_clean(dataset: Dataset, table_name: str) -> Table:
    """Load and clean an open number's table from a dataset.

    This function resets the indices, maps Open Number's codes to OWID country names, checks columns and standardizes their names.

    Parameters
    ----------
    ds: Dataset
        Open number's dataset
    table_name: str
        Table name in `dataset`
    Returns
    -------
    t: Table
        Cleaned table.

    Raises
    ------
        Exception: If table shape is not as expected.
        KeyError: If columns are missing.

    Usage:
        >>> from owid.catalog import Dataset
        >>> from etl.paths import DATA_DIR
        >>> from etl.steps.open_numbers import on_table_clean_fast
        >>> d = Dataset(DATA_DIR / "meadow/open_numbers/latest/open_numbers__world_development_indicators")
        >>> df = on_table_clean_fast(d, "it_net_user_zs")
    """
    df = dataset[table_name].reset_index()
    # Sanity checks
    if (ncols := df.shape[1]) != 3:
        raise Exception(f"Table must have 3 columns (including original indices). Instead it has {ncols} columns.")
    if "geo" not in df.columns:
        if "global" in df.columns:
            raise KeyError("This is a global dataset! Does not have column 'geo' but 'global' instead.")
        raise KeyError(f"Table should have column named 'geo'! Found columns are {df.columns}.")
    if "time" not in df.columns:
        raise KeyError(f"Table should have column named 'time'! Found columns are {df.columns}.")
    if table_name not in df.columns:
        raise KeyError(
            f"Table should have column with the metric of interest (same name as the table): '{table_name}'! Found"
            f" columns are {df.columns}."
        )
    # First clean
    df = df.rename(
        columns={
            "time": "year",
        }
    ).assign(country=iso_gm2owid(df.geo))
    countries_missing = df.loc[df.country.isna(), "geo"].unique().tolist()
    log.warning(f"Countries missing (listed by Gapminder code): {', '.join(countries_missing)}")

    df = df.dropna(subset=["country"]).drop(["geo"], axis=1)
    return df[["country", "year", table_name]]
