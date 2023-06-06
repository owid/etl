# Oil Spills

## About this dataset

ITOPF maintains a database of oil spills from tank vessels, including combined carriers, FPSOs and barges. This contains information on accidental spillages of persistent and non-persistent hydrocarbon oil since 1970, except those resulting from acts of war.


## Dependencies

This diagram shows the inputs that are used in constructing this table. Dependencies are tracked
on the level of "datasets" only, where one "dataset" is a collection of tables in one directory.

To make sense of these dependencies, it helps to understand our terminology of the different processing levels,
namely snaphots/walden, then meadow, then garden and finally grapher. See [our documentation](https://docs.owid.io/projects/etl/en/latest/) for more details.

```mermaid
flowchart TD
    1["data://meadow/itopf/2023-05-18/oil_spills"] --> 0["data://garden/itopf/2023-05-18/oil_spills"]
    click 1 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/itopf/2023-05-18/oil_spills.py"
    2["snapshot://itopf/2023-05-18/oil_spills.pdf"] --> 1["data://meadow/itopf/2023-05-18/oil_spills"]
    click 2 href "https://github.com/owid/etl/tree/master/itopf/2023-05-18/oil_spills.pdf"

```
