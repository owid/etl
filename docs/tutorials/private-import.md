!!! warning "This tutorial might be partial or incomplete. Please check with the team if you have questions."

Most of the steps have private versions with `-private` suffix (e.g. `data-private://...`, `walden-private://...`) that remember and enforce a dataset level `is_public` flag.

When publishing, the index is public, but tables in the index that are private are only available over s3 with appropriate credentials. `owid-catalog-py` is also updated to support these private datasets and supports fetching them over s3 transparently.

### Uploading private data to walden

It is possible to upload private data to walden so that only those with S3 credentials would be able to download it.

```python
from owid.walden import Dataset

local_file = 'private_test.csv'
metadata = {
  'name': 'private_test',
  'short_name': 'private_test',
  'description': 'testing private walden data',
  'source_name': 'test',
  'url': 'test',
  'license_url': 'test',
  'date_accessed': '2022-02-07',
  'file_extension': 'csv',
  'namespace': 'private',
  'publication_year': 2021,
}

# upload the local file to Walden's cache
dataset = Dataset.copy_and_create(local_file, metadata)
# upload it as private file to S3
url = dataset.upload(public=False)
# update PUBLIC walden index with metadata
dataset.save()
```

## Running private ETL

`--private` flag rebuilds everything including private datasets

```
etl --private
reindex
publish --private
```
