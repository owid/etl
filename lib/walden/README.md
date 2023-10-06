# walden

[![Build status](https://badge.buildkite.com/a2012d475d9eed5ae6d85b9362acc07aa5cab265a810d9a37b.svg)](https://buildkite.com/our-world-in-data/walden-unit-tests)


_A prototype catalog of all upstream datasets used to build Our World in Data._

## Goals

Walden is a _source catalog_, meaning a catalog of datasets that we use to build OWID, in their raw form. Today these datasets are committed to the [importers](https://github.com/owid/importers) repo, but this cannot be done if they are too large, and is unlikely to scale well into the future. Walden aims to replace this pattern in user-friendly way, and to be the foundation stage of a highly repeatable data infrastructure.

## Catalog structure

The catalog is meant to be both human and machine readable. It is just JSON files living in the `index/` directory, where each file tells you about a dataset and gives a URL where you can download it from.

For example, suppose the UN FAO provides a file `VeryImportantData.xlsx` in 2019. Then inside the `index/un_fao/2019/` folder there will be a file `very_important_data.json` that looks like this:

```json
{
  "md5": "a0dc1033f5d8952739497fb0932ff240",
  "namespace": "un_fao",
  "short_name": "very_important_data",
  "description": "...",
  "publication_year": 2019,
  "owid_data_url": "https://walden.nyc.digitalocean.com/un_fao/2019/very_important_data.xlsx",
  ...
}
```

To get the data locally, you can run "make fetch" to download everything to `data/`, or programmatically read the catalog and fetch just what you need.

## Working with the catalog

You need Python 3.8+ to use this repository, with `poetry` installed (`pip install poetry`).

Then install the environment using

```
poetry install
```

### The basics

You may then run tests with:

```
make test
```

Fetch all data locally with:

```
make fetch
```

Or simply run `make` to see available commands.

## Adding to the catalog

WARNING: the catalog is public, so do not add private or embargoed data to the catalog at this time

### Via API

Walden provides a Python API for adding new datasets:

```python
from owid.walden import Dataset

local_file = 'some_downloaded_file.xlsx'
metadata = {
  'name': 'my_special_dataset',
  'namespace': 'some_institution',
  'publication_year': 2021,
  ...
}

# upload the local file to Walden's cache
dataset = Dataset.copy_and_create(local_file, metadata)

# upload the file from URL to Walden's cache, make sure
# to set `source_data_url`
# dataset = Dataset.download_and_create(metadata)

# save the JSON metadata locally in the right place
dataset.save()

# upload the file to S3 as either public or private
dataset.upload(public=True)
```

or simply

```python
from owid.walden import add_to_catalog
add_to_catalog(metadata, local_file, upload=True)
```

You have to commit and push the JSON file in the `index/` folder to make it available to others:

```bash
git add index/
git commit -m 'Add my shiny new dataset'
git push
```

It is a good practice to add the script you used for uploading the file to walden to [walden/ingests](https://github.com/owid/walden/tree/master/ingests) folder.

### Manually

You can also do all of this manually:

1. **Create a JSON metadata file.** Create a JSON metadata file at `index/<namespace>/<publication_year>/<short_name>.json` and fill out as many of the fields as you can in [the schema](https://github.com/owid/walden/blob/master/owid/walden/schema.json).
2. **Calculate and add the checksum.** You should download the actual data file locally, calculate its md5 checksum, and that to the metadata too (e.g. `md5 -q myfilename.xlsx`).
3. **Upload to the remote cache.** Upload your datafile to DigitalOcean spaces, (e.g. `s3cmd put --acl-public my_filename.xlsx s3://walden/<namespace>/<publication_year>/<short_name>.xlsx`), and add the URL it gives you to your metadata file as the `owid_data_url` variable
4. **Check it's correct.** Run `make test` to check your file against the schema, and if necessary, `make format` to reformat it.
5. **Ship it!** Commit and push.

## Using the catalog

A basic Python API is available, suggestions for improvement are most welcome.

The most basic method to get the data from catalog is:

```python
from owid.walden import Catalog

dataset = Catalog().find_one(namespace="wb", version="2021-07-01", short_name="wb_income")
local_path = dataset.ensure_downloaded()
df = pd.read_csv(local_path)  # assuming the file is a csv
```

You can also iterate over all datasets in the catalog:

```python
from owid.walden import Catalog

catalog = Catalog()  # just a list of datasets, really

for dataset in catalog:
    # all schema attributes are available directly on the object
    print(dataset.short_name)

    # fetch the data file locally to ~/.owid/walden/
    dataset.ensure_downloaded()

    # do something with the data file
    do_something(dataset.local_path)
```

## TODO

First prototype

- [x] (Lars) Validate entries against the schema with one command
- [x] (Lars) One command to fetch all the data
- [x] (Lars) A dedicated space for our source files
- [x] (Lars) Access for all data team members to the dedicated space
- [x] (Lucas) Trial using the index for FAO - food security
  - [x] Manually add JSON files for each data file
  - [x] Upload copies of data files to our cache
- [x] Run "make test" equivalent as a Github action
- [ ] ~~Make an interactive helper script for adding a new file to the catalog~~

Break, share with the team. If it's good, continue:

- [ ] (Ed/Lucas) Add our existing importers datasets to the index
  - [ ] faostat
  - [ ] ihme_sdg
  - [ ] povcal
  - [ ] un_wpp
  - [ ] vdem
  - [ ] who_gho
  - [ ] worldbank
  - [ ] worldbank_wdi
