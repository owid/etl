# Energy dataset (OWID, 2023)

## About this dataset

OWID Energy dataset.

This dataset will be loaded by <a href="https://github.com/owid/energy-data">the energy-data repository</a>, to create a csv file of the dataset that can be downloaded in one click.


## Dependencies

This diagram shows the inputs that are used in constructing this table. Dependencies are tracked
on the level of "datasets" only, where one "dataset" is a collection of tables in one directory.

To make sense of these dependencies, it helps to understand our terminology of the different processing levels,
namely snaphots/walden, then meadow, then garden and finally grapher. See [our documentation](https://docs.owid.io/projects/etl/en/latest/) for more details.

```mermaid
flowchart TD
    1["data://garden/bp/2023-02-20/energy_mix"] --> 0["data://garden/energy/2023-06-01/owid_energy"]
    click 1 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/bp/2023-02-20/energy_mix.py"
    9["data://garden/energy/2023-06-01/fossil_fuel_production"] --> 0["data://garden/energy/2023-06-01/owid_energy"]
    click 9 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/energy/2023-06-01/fossil_fuel_production.py"
    2["data://garden/energy/2023-06-01/primary_energy_consumption"] --> 0["data://garden/energy/2023-06-01/owid_energy"]
    click 2 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/energy/2023-06-01/primary_energy_consumption.py"
    22["data://garden/demography/2023-03-31/population"] --> 0["data://garden/energy/2023-06-01/owid_energy"]
    click 22 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/demography/2023-03-31/population.py"
    40["data://garden/ggdc/2020-10-01/ggdc_maddison"] --> 0["data://garden/energy/2023-06-01/owid_energy"]
    click 40 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/ggdc/2020-10-01/ggdc_maddison.py"
    25["data://garden/energy/2023-06-01/electricity_mix"] --> 0["data://garden/energy/2023-06-01/owid_energy"]
    click 25 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/energy/2023-06-01/electricity_mix.py"
    8["data://garden/regions/2023-01-01/regions"] --> 0["data://garden/energy/2023-06-01/owid_energy"]
    click 8 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/regions/2023-01-01/regions.py"
    10["data://garden/demography/2022-12-08/population"] --> 1["data://garden/bp/2023-02-20/energy_mix"]
    click 10 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/demography/2022-12-08/population.py"
    3["data://garden/bp/2022-12-28/statistical_review"] --> 1["data://garden/bp/2023-02-20/energy_mix"]
    click 3 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/bp/2022-12-28/statistical_review.py"
    28["data://garden/wb/2021-07-01/wb_income"] --> 1["data://garden/bp/2023-02-20/energy_mix"]
    click 28 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/wb/2021-07-01/wb_income.py"
    32["data://garden/eia/2022-07-27/energy_consumption"] --> 2["data://garden/energy/2023-06-01/primary_energy_consumption"]
    click 32 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/eia/2022-07-27/energy_consumption.py"
    22["data://garden/demography/2023-03-31/population"] --> 2["data://garden/energy/2023-06-01/primary_energy_consumption"]
    click 22 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/demography/2023-03-31/population.py"
    3["data://garden/bp/2022-12-28/statistical_review"] --> 2["data://garden/energy/2023-06-01/primary_energy_consumption"]
    click 3 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/bp/2022-12-28/statistical_review.py"
    40["data://garden/ggdc/2020-10-01/ggdc_maddison"] --> 2["data://garden/energy/2023-06-01/primary_energy_consumption"]
    click 40 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/ggdc/2020-10-01/ggdc_maddison.py"
    21["data://garden/bp/2022-07-11/statistical_review"] --> 3["data://garden/bp/2022-12-28/statistical_review"]
    click 21 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/bp/2022-07-11/statistical_review.py"
    4["backport://backport/owid/latest/dataset_5650_statistical_review_of_world_energy__bp__2022"] --> 3["data://garden/bp/2022-12-28/statistical_review"]
    click 4 href "https://github.com/owid/etl/tree/master/etl/steps/data/backport/owid/latest/dataset_5650_statistical_review_of_world_energy__bp__2022.py"
    28["data://garden/wb/2021-07-01/wb_income"] --> 3["data://garden/bp/2022-12-28/statistical_review"]
    click 28 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/wb/2021-07-01/wb_income.py"
    5["data://garden/owid/latest/key_indicators"] --> 3["data://garden/bp/2022-12-28/statistical_review"]
    click 5 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/owid/latest/key_indicators.py"
    8["data://garden/regions/2023-01-01/regions"] --> 3["data://garden/bp/2022-12-28/statistical_review"]
    click 8 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/regions/2023-01-01/regions.py"
    10["data://garden/demography/2022-12-08/population"] --> 5["data://garden/owid/latest/key_indicators"]
    click 10 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/demography/2022-12-08/population.py"
    6["data://open_numbers/open_numbers/latest/open_numbers__world_development_indicators"] --> 5["data://garden/owid/latest/key_indicators"]
    click 6 href "https://github.com/owid/etl/tree/master/etl/steps/data/open_numbers/open_numbers/latest/open_numbers__world_development_indicators.py"
    8["data://garden/regions/2023-01-01/regions"] --> 5["data://garden/owid/latest/key_indicators"]
    click 8 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/regions/2023-01-01/regions.py"
    28["data://garden/wb/2021-07-01/wb_income"] --> 5["data://garden/owid/latest/key_indicators"]
    click 28 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/wb/2021-07-01/wb_income.py"
    7["github://open-numbers/ddf--open_numbers--world_development_indicators"] --> 6["data://open_numbers/open_numbers/latest/open_numbers__world_development_indicators"]
    click 7 href "https://github.com/open-numbers/ddf--open_numbers--world_development_indicators"
    22["data://garden/demography/2023-03-31/population"] --> 9["data://garden/energy/2023-06-01/fossil_fuel_production"]
    click 22 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/demography/2023-03-31/population.py"
    3["data://garden/bp/2022-12-28/statistical_review"] --> 9["data://garden/energy/2023-06-01/fossil_fuel_production"]
    click 3 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/bp/2022-12-28/statistical_review.py"
    26["data://garden/shift/2022-07-18/fossil_fuel_production"] --> 9["data://garden/energy/2023-06-01/fossil_fuel_production"]
    click 26 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/shift/2022-07-18/fossil_fuel_production.py"
    11["data://open_numbers/open_numbers/latest/gapminder__systema_globalis"] --> 10["data://garden/demography/2022-12-08/population"]
    click 11 href "https://github.com/owid/etl/tree/master/etl/steps/data/open_numbers/open_numbers/latest/gapminder__systema_globalis.py"
    15["data://garden/hyde/2017/baseline"] --> 10["data://garden/demography/2022-12-08/population"]
    click 15 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/hyde/2017/baseline.py"
    13["data://garden/gapminder/2019-12-10/population"] --> 10["data://garden/demography/2022-12-08/population"]
    click 13 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/gapminder/2019-12-10/population.py"
    37["data://garden/un/2022-07-11/un_wpp"] --> 10["data://garden/demography/2022-12-08/population"]
    click 37 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/un/2022-07-11/un_wpp.py"
    12["github://open-numbers/ddf--gapminder--systema_globalis"] --> 11["data://open_numbers/open_numbers/latest/gapminder__systema_globalis"]
    click 12 href "https://github.com/open-numbers/ddf--gapminder--systema_globalis"
    14["data://meadow/gapminder/2019-12-10/population"] --> 13["data://garden/gapminder/2019-12-10/population"]
    click 14 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/gapminder/2019-12-10/population.py"
    17["walden://gapminder/2019-12-10/population"] --> 14["data://meadow/gapminder/2019-12-10/population"]
    click 17 href "https://github.com/owid/walden/tree/master/owid/walden/index/gapminder/2019-12-10/population"
    16["data://meadow/hyde/2017/baseline"] --> 15["data://garden/hyde/2017/baseline"]
    click 16 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/hyde/2017/baseline.py"
    18["data://meadow/hyde/2017/general_files"] --> 16["data://meadow/hyde/2017/baseline"]
    click 18 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/hyde/2017/general_files.py"
    20["snapshot://hyde/2017/baseline.zip"] --> 16["data://meadow/hyde/2017/baseline"]
    click 20 href "https://github.com/owid/etl/tree/master/hyde/2017/baseline.zip"
    19["snapshot://hyde/2017/general_files.zip"] --> 18["data://meadow/hyde/2017/general_files"]
    click 19 href "https://github.com/owid/etl/tree/master/hyde/2017/general_files.zip"
    24["backport://backport/owid/latest/dataset_5347_statistical_review_of_world_energy__bp__2021"] --> 21["data://garden/bp/2022-07-11/statistical_review"]
    click 24 href "https://github.com/owid/etl/tree/master/etl/steps/data/backport/owid/latest/dataset_5347_statistical_review_of_world_energy__bp__2021.py"
    5["data://garden/owid/latest/key_indicators"] --> 21["data://garden/bp/2022-07-11/statistical_review"]
    click 5 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/owid/latest/key_indicators.py"
    8["data://garden/regions/2023-01-01/regions"] --> 21["data://garden/bp/2022-07-11/statistical_review"]
    click 8 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/regions/2023-01-01/regions.py"
    28["data://garden/wb/2021-07-01/wb_income"] --> 21["data://garden/bp/2022-07-11/statistical_review"]
    click 28 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/wb/2021-07-01/wb_income.py"
    23["data://garden/gapminder/2023-03-31/population"] --> 22["data://garden/demography/2023-03-31/population"]
    click 23 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/gapminder/2023-03-31/population.py"
    11["data://open_numbers/open_numbers/latest/gapminder__systema_globalis"] --> 22["data://garden/demography/2023-03-31/population"]
    click 11 href "https://github.com/owid/etl/tree/master/etl/steps/data/open_numbers/open_numbers/latest/gapminder__systema_globalis.py"
    15["data://garden/hyde/2017/baseline"] --> 22["data://garden/demography/2023-03-31/population"]
    click 15 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/hyde/2017/baseline.py"
    37["data://garden/un/2022-07-11/un_wpp"] --> 22["data://garden/demography/2023-03-31/population"]
    click 37 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/un/2022-07-11/un_wpp.py"
    34["data://meadow/gapminder/2023-03-31/population"] --> 23["data://garden/gapminder/2023-03-31/population"]
    click 34 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/gapminder/2023-03-31/population.py"
    22["data://garden/demography/2023-03-31/population"] --> 25["data://garden/energy/2023-06-01/electricity_mix"]
    click 22 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/demography/2023-03-31/population.py"
    3["data://garden/bp/2022-12-28/statistical_review"] --> 25["data://garden/energy/2023-06-01/electricity_mix"]
    click 3 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/bp/2022-12-28/statistical_review.py"
    42["data://garden/ember/2023-06-01/combined_electricity"] --> 25["data://garden/energy/2023-06-01/electricity_mix"]
    click 42 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/ember/2023-06-01/combined_electricity.py"
    8["data://garden/regions/2023-01-01/regions"] --> 26["data://garden/shift/2022-07-18/fossil_fuel_production"]
    click 8 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/regions/2023-01-01/regions.py"
    27["data://meadow/shift/2022-07-18/fossil_fuel_production"] --> 26["data://garden/shift/2022-07-18/fossil_fuel_production"]
    click 27 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/shift/2022-07-18/fossil_fuel_production.py"
    31["walden://shift/2022-07-18/fossil_fuel_production"] --> 27["data://meadow/shift/2022-07-18/fossil_fuel_production"]
    click 31 href "https://github.com/owid/walden/tree/master/owid/walden/index/shift/2022-07-18/fossil_fuel_production"
    29["data://meadow/wb/2021-07-01/wb_income"] --> 28["data://garden/wb/2021-07-01/wb_income"]
    click 29 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/wb/2021-07-01/wb_income.py"
    30["walden://wb/2021-07-01/wb_income"] --> 29["data://meadow/wb/2021-07-01/wb_income"]
    click 30 href "https://github.com/owid/walden/tree/master/owid/walden/index/wb/2021-07-01/wb_income"
    33["data://meadow/eia/2022-07-27/energy_consumption"] --> 32["data://garden/eia/2022-07-27/energy_consumption"]
    click 33 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/eia/2022-07-27/energy_consumption.py"
    5["data://garden/owid/latest/key_indicators"] --> 32["data://garden/eia/2022-07-27/energy_consumption"]
    click 5 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/owid/latest/key_indicators.py"
    8["data://garden/regions/2023-01-01/regions"] --> 32["data://garden/eia/2022-07-27/energy_consumption"]
    click 8 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/regions/2023-01-01/regions.py"
    36["walden://eia/2022-07-27/international_energy_data"] --> 33["data://meadow/eia/2022-07-27/energy_consumption"]
    click 36 href "https://github.com/owid/walden/tree/master/owid/walden/index/eia/2022-07-27/international_energy_data"
    35["snapshot://gapminder/2023-03-31/population.xlsx"] --> 34["data://meadow/gapminder/2023-03-31/population"]
    click 35 href "https://github.com/owid/etl/tree/master/gapminder/2023-03-31/population.xlsx"
    38["data://meadow/un/2022-07-11/un_wpp"] --> 37["data://garden/un/2022-07-11/un_wpp"]
    click 38 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/un/2022-07-11/un_wpp.py"
    39["snapshot://un/2022-07-11/un_wpp.zip"] --> 38["data://meadow/un/2022-07-11/un_wpp"]
    click 39 href "https://github.com/owid/etl/tree/master/un/2022-07-11/un_wpp.zip"
    41["snapshot://ggdc/2020-10-01/ggdc_maddison.xlsx"] --> 40["data://garden/ggdc/2020-10-01/ggdc_maddison"]
    click 41 href "https://github.com/owid/etl/tree/master/ggdc/2020-10-01/ggdc_maddison.xlsx"
    43["data://garden/ember/2023-06-01/yearly_electricity"] --> 42["data://garden/ember/2023-06-01/combined_electricity"]
    click 43 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/ember/2023-06-01/yearly_electricity.py"
    46["data://garden/ember/2022-08-01/european_electricity_review"] --> 42["data://garden/ember/2023-06-01/combined_electricity"]
    click 46 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/ember/2022-08-01/european_electricity_review.py"
    44["data://meadow/ember/2023-06-01/yearly_electricity"] --> 43["data://garden/ember/2023-06-01/yearly_electricity"]
    click 44 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/ember/2023-06-01/yearly_electricity.py"
    8["data://garden/regions/2023-01-01/regions"] --> 43["data://garden/ember/2023-06-01/yearly_electricity"]
    click 8 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/regions/2023-01-01/regions.py"
    22["data://garden/demography/2023-03-31/population"] --> 43["data://garden/ember/2023-06-01/yearly_electricity"]
    click 22 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/demography/2023-03-31/population.py"
    47["data://garden/wb/2023-04-30/income_groups"] --> 43["data://garden/ember/2023-06-01/yearly_electricity"]
    click 47 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/wb/2023-04-30/income_groups.py"
    45["snapshot://ember/2023-06-01/yearly_electricity.csv"] --> 44["data://meadow/ember/2023-06-01/yearly_electricity"]
    click 45 href "https://github.com/owid/etl/tree/master/ember/2023-06-01/yearly_electricity.csv"
    8["data://garden/regions/2023-01-01/regions"] --> 46["data://garden/ember/2022-08-01/european_electricity_review"]
    click 8 href "https://github.com/owid/etl/tree/master/etl/steps/data/garden/regions/2023-01-01/regions.py"
    50["data://meadow/ember/2022-08-01/european_electricity_review"] --> 46["data://garden/ember/2022-08-01/european_electricity_review"]
    click 50 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/ember/2022-08-01/european_electricity_review.py"
    48["data://meadow/wb/2023-04-30/income_groups"] --> 47["data://garden/wb/2023-04-30/income_groups"]
    click 48 href "https://github.com/owid/etl/tree/master/etl/steps/data/meadow/wb/2023-04-30/income_groups.py"
    49["snapshot://wb/2023-04-30/income_groups.xlsx"] --> 48["data://meadow/wb/2023-04-30/income_groups"]
    click 49 href "https://github.com/owid/etl/tree/master/wb/2023-04-30/income_groups.xlsx"
    51["walden://ember/2022-02-01/european_electricity_review"] --> 50["data://meadow/ember/2022-08-01/european_electricity_review"]
    click 51 href "https://github.com/owid/walden/tree/master/owid/walden/index/ember/2022-02-01/european_electricity_review"

```
