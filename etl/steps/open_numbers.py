#
#  open_numbers.py
#  etl
#

"""
Convert repositories with data in DDF format to OWID's Dataset and Table
format.
"""

from multiprocessing import Pool
from pathlib import Path
from typing import Dict, List, Tuple, cast
import hashlib
from owid.catalog.meta import Source
from owid.catalog import utils
import datetime as dt
import tempfile

import pandas as pd
from etl.git import GithubRepo
import frictionless
from frictionless.exception import FrictionlessException

from owid.catalog import Dataset, Table


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
    ds.metadata.sources = [
        Source(url=repo.github_url, date_accessed=str(dt.date.today()))
    ]

    if package.description and package.title != package.description:
        ds.metadata.description = package.description
    ds.save()

    # name remapping
    resource_map = remap_names(package.resources)

    # copy tables one by one
    with Pool() as pool:
        args = [
            (ds, repo, short_name, resources)
            for short_name, resources in resource_map.items()
        ]
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

    ds.add(t)


def load_table(resource: frictionless.Resource) -> pd.DataFrame:
    df = cast(pd.DataFrame, resource.to_pandas())

    # use smaller, more accurate column types that minimise space
    if "global" in df.columns:
        df["geo"] = df.pop("global")

    return df


def load_and_combine(
    path: Path, resources: List[frictionless.Resource]
) -> pd.DataFrame:
    first = True
    primary_key: List[str]
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
        df = pd.read_csv(f.name)

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
