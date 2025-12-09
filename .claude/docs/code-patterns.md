# ETL Code Patterns

## Standard Garden Step

```python
from etl.helpers import PathFinder
from etl.data_helpers import geo

paths = PathFinder(__file__)

def run() -> None:
    ds_input = paths.load_dataset("input_dataset")
    tb = ds_input["table_name"].reset_index()
    tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)
    tb = tb.format(short_name=paths.short_name)
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
```

## Ad-hoc Data Exploration

```python
from etl.snapshot import Snapshot
snap = Snapshot("who/latest/fluid.csv")
tb = snap.read_csv()
```

## Geographic Harmonization

Always use `geo.harmonize_countries()` for country name standardization:

```python
from etl.data_helpers import geo
tb = geo.harmonize_countries(tb, countries_file=paths.country_mapping_path)
```

## YAML Editing

Use `ruamel_load`/`ruamel_dump` to preserve comments:

```python
from etl.files import ruamel_load, ruamel_dump

data = ruamel_load(file_path)
data['key'] = new_value
with open(file_path, 'w') as f:
    f.write(ruamel_dump(data))
```

## CLI Scripts

Always use Click (not ArgumentParser):

```python
import click

@click.command()
@click.option("--dry-run", is_flag=True)
def main(dry_run: bool):
    """Brief description."""
    pass

if __name__ == "__main__":
    main()
```

## Catalog System

- **Dataset**: Container for multiple tables with shared metadata
- **Table**: pandas.DataFrame subclass with rich metadata per column
- **Variable**: pandas.Series subclass with variable-specific metadata
- Content-based checksums for change detection
