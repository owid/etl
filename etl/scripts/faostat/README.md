# FAOSTAT data

[FAOSTAT (Food and Agriculture Organization Corporate Statistical Database)](https://www.fao.org/faostat/en/#home) 
provides free access to food and agriculture data from 1961 to the most recent year available.

## Workflow to keep data up-to-date

These are the steps OWID follows to ensure that FAOSTAT data is up-to-date, or to update one or more datasets for
which there is new data (let us call the new dataset version to be created `YYYY-MM-DD`):

0. Activate the etl poetry environment (from the root folder of the etl repository):
```bash
  poetry shell
```
1. Execute the walden ingest script, to fetch data for any dataset that may have been updated in FAOSTAT.
If no dataset requires an update, the workflow stops here.

    Note: This can be executed with the `-r` flag to simply check for updates without writing anything.
```bash
python vendor/walden/ingests/faostat.py
```
2. Execute this script for the meadow channel.
```bash
python etl/scripts/faostat/create_new_steps.py -c meadow
```
3. Run the new etl meadow steps, to generate the meadow datasets.
```bash
etl meadow/faostat/YYYY-MM-DD
```
4. Run this script again for the garden channel.
```bash
python etl/scripts/faostat/create_new_steps.py -c garden
```
5. Run the new etl garden steps, to generate the garden datasets.
```bash
etl garden/faostat/YYYY-MM-DD
```
6. Run this script again for the grapher channel.
```bash
python etl/scripts/faostat/create_new_steps.py -c grapher
```
7. Run the new etl grapher steps, to generate the grapher charts.
```bash
etl faostat/YYYY-MM-DD --grapher
```
8. Use OWID's internal approval tool to visually inspect changes between the old and new versions of updated charts, and
accept or reject changes.
