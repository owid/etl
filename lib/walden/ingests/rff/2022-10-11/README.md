# World carbon pricing and emissions-weighted carbon price datasets

## Description
Scripts to be executed to ingest all required files:
* `emissions_weighted_carbon_price__coverage.py`
* `emissions_weighted_carbon_price__economy.py`
* `ipcc_codes.py`
* `world_carbon_pricing__subnational.py`
* `world_carbon_pricing.py`

The previous scripts will ingest all necessary data from:
1. [World Carbon Pricing Database](https://github.com/g-dolphin/WorldCarbonPricingDatabase/tree/master/_dataset/).
2. [Emissions-weighted Carbon Price](https://github.com/g-dolphin/ECP/tree/master/_dataset).

## Changelog
The only differences with respect to the previous version are:
1. The 'publication_date' has been updated in all *.yml files.
2. The "world_carbon_pricing__sources" and "emissions_weighted_carbon_price__sectors" files are not ingested anymore (since, for now, they are not used).
3. This README was created.
