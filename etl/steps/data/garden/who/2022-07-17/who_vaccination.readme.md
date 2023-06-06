## About this dataset



## Dependencies

This diagram shows the inputs that are used in constructing this table. Dependencies are tracked
on the level of "datasets" only, where one "dataset" is a collection of tables in one directory.

To make sense of these dependencies, it helps to understand our terminology of the different processing levels,
namely snaphots/walden, then meadow, then garden and finally grapher. See [our documentation](https://docs.owid.io/projects/etl/en/latest/) for more details.

```mermaid
flowchart TD
    1["data://garden/un/2022-07-11/un_wpp"] --> 0["data://garden/who/2022-07-17/who_vaccination"]
    click 1 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/un/2022-07-11/un_wpp.py"
    4["data://meadow/who/2022-07-17/who_vaccination"] --> 0["data://garden/who/2022-07-17/who_vaccination"]
    click 4 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/who/2022-07-17/who_vaccination.py"
    2["data://meadow/un/2022-07-11/un_wpp"] --> 1["data://garden/un/2022-07-11/un_wpp"]
    click 2 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/un/2022-07-11/un_wpp.py"
    3["snapshot://un/2022-07-11/un_wpp.zip"] --> 2["data://meadow/un/2022-07-11/un_wpp"]
    click 3 href "https://github.com/owid/etl/tree/master/un/2022-07-11/un_wpp.zip"
    5["walden://who/2022-07-17/who_vaccination"] --> 4["data://meadow/who/2022-07-17/who_vaccination"]
    click 5 href "https://github.com/owid/walden/tree/master/owid/walden/index/who/2022-07-17/who_vaccination"

```
