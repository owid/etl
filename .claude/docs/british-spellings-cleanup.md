# Cleaning up British spellings in ETL metadata

OWID writes in American English, but British spellings have accumulated across step
metadata and snapshot `.dvc` files. This document is the worklist for fixing them.

It is a one-off task. Delete this file once the cleanup is finished.

## Scope

**In scope:** prose that a reader can see. Indicator `title`, `description_short`,
`description_key`, `description_processing`, `description_from_producer`, `unit`,
`short_unit`, dataset descriptions, `grapher_config` titles/subtitles/notes, and the
`origin.description` / `origin.description_snapshot` fields of snapshot `.dvc` files.

Fixing `description_from_producer` is deliberate. Americanizing a producer's spelling is
one of the minor edits the metadata reference allows, alongside typo fixes.

**Out of scope — do not change:**

- **Official names.** `producer`, `attribution`, `attribution_short`, `citation_full`,
  `license.name`, and any proper noun in prose. "International Labour Organization",
  "Organisation for Economic Co-operation and Development", "United Nations Environment
  Programme", "Joint Research Centre" and "Open Government Licence" are correct as written.
  As a rule of thumb, a capitalized British word is part of a name.
- **Identifiers.** Step short names, table names, column names, and catalog paths.
  Renaming `geography/*/neighbours` or `education/latest/enrolment_rates` mints new
  catalog paths and variable IDs, which is a migration, not a spelling fix.
  `garden/geography/2026-01-27/neighbours` and `external/owid_grapher/latest/neighbours`
  match only on identifiers, so skip them entirely. Elsewhere a single file carries both:
  `snapshots/unesco/2024-11-21/enrolment_rates.csv.dvc` has `enrolment` in its file name,
  which stays, and in its `description` prose, which changes.
- **`aluminium`.** This is the IUPAC name, not a British-only variant. Leave it.
- **`grey`.** Almost every occurrence is a matplotlib or plotly color name in code.
- **Words that are already correct.** `analysis`, `analyses`, and `analyst` are correct
  American English. Only `analyse`, `analysed` and `analysing` are British.

## Why this cannot be one pull request

Editing a `.dvc` or a `.meta.yml` changes the step checksum, so every edited step and
everything downstream of it rebuilds, and every chart it feeds shows up in chart diff.
Doing all of it at once produces a chart diff far too large to review honestly for a
spelling change.

**Work namespace by namespace, one pull request each.** For each one:

1. Edit the files listed for that namespace.
2. Run the affected steps: `.venv/bin/etlr <step> --private`.
3. Push and check chart diff on staging. Every difference should be text only. A change
   in any number means something other than a spelling was edited: stop and investigate.
4. Keep the pull request to a single namespace, so a reviewer can scan it in one pass.

## What to do first

Start with a small, self-contained namespace to establish the shape of the change, then
scale up. Leave `covid` and `backport` for last, or skip them: see the note below.

Regenerate the list before starting, since it goes stale as other pull requests land. The
scan looks for these forms, case-insensitively, on word boundaries, ignoring any match
that is capitalized or sits on a `producer:` / `attribution:` / `citation_full:` /
`name:` / `title_snapshot:` / `url*:` line:

```
metre(s) kilometre(s) millimetre(s) centimetre(s) litre(s) programme(s) colour(s)
coloured behaviour(s) behavioural neighbour(s) neighbouring neighbourhood fibre(s)
sulphur* modelling modelled travelling travelled labelling labelled fuelled ageing
analyse analysed analysing utilis* recognis* categoris* prioritis* organis* harmonis*
centre(s) defence licence favour* "per cent" enrolment fulfil storey(s) skilful
instalment(s)
```

## Size

399 files across 83 namespaces, of which:

- **306 files** belong to steps still active in the DAG. These are the ones that matter.
- **93 files** belong to steps no longer in any active `dag/*.yml`, mostly `backport`
  (15), old `health` (10) and `wb` (9) versions. Fixing these changes nothing a reader
  sees. Skip them unless the step is revived.

The `covid` namespace alone accounts for 42 active files. Those datasets are frozen, so
their spellings are visible but will never be republished through a normal update. Decide
whether they are worth a rebuild before starting on them.

Most common words to fix: `modelled` (149), `enrolment` (112), `programme(s)` (186),
`behaviour*` (160), `modelling` (52), `kilometre(s)` (57), `metres` (34), `per cent` (31),
`defence` (28).

## Worklist

Every file below has at least one lowercase British spelling outside a name field.
The words listed are what the scan found in that file; check the surrounding text
yourself, since the scan cannot tell prose from an identifier.



### `un` — 43 files

- `etl/steps/data/garden/un/2023-08-16/un_sdg.py` — labelling
- `etl/steps/data/garden/un/2024-07-08/maternal_mortality.meta.yml` — modelling
- `etl/steps/data/garden/un/2024-07-12/un_wpp.meta.yml` — kilometre, per cent
- `etl/steps/data/garden/un/2024-07-25/refugee_data.meta.yml` — programmes
- `etl/steps/data/garden/un/2024-08-27/un_sdg.py` — labelling
- `etl/steps/data/garden/un/2024-12-02/un_wpp_lt.py` — harmonise
- `etl/steps/data/garden/un/2025-04-25/long_run_child_mortality.meta.yml` — labelled
- `etl/steps/data/garden/un/2025-05-07/undp_hdr.meta.yml` — harmonised
- `etl/steps/data/garden/un/2025-07-03/refugee_data.meta.yml` — programmes
- `etl/steps/data/garden/un/2025-10-29/un_sdg.py` — labelling
- `etl/steps/data/garden/un/2026-02-03/ilostat.meta.yml` — modelled
- `etl/steps/data/garden/un/2026-02-03/ilostat.py` — modelled
- `etl/steps/data/garden/un/2026-02-17/wup_national_definitions.meta.yml` — modelled
- `etl/steps/data/garden/un/2026-06-09/long_run_child_mortality.meta.yml` — labelled
- `etl/steps/data/grapher/un/2023-01-24/un_sdg.sources.json` — modelled, programme, programmes
- `etl/steps/data/grapher/un/2023-08-16/un_sdg.sources.json` — modelled, programme, programmes
- `etl/steps/data/grapher/un/2024-08-27/un_sdg.meta.yml` — favour, modelled, organisations
- `etl/steps/data/grapher/un/2024-08-27/un_sdg.sources.json` — modelled, programme, programmes
- `etl/steps/data/grapher/un/2025-10-29/un_sdg.meta.yml` — favour, modelled, organisations
- `etl/steps/data/grapher/un/2025-10-29/un_sdg.sources.json` — modelled, programme, programmes
- `snapshots/un/2024-10-21/census_dates.csv.dvc` — programmes
- `snapshots/un/2024-12-02/un_wpp_lt_all.csv.dvc` — per cent
- `snapshots/un/2024-12-02/un_wpp_lt_f.csv.dvc` — per cent
- `snapshots/un/2024-12-02/un_wpp_lt_m.csv.dvc` — per cent
- `snapshots/un/2024-12-02/un_wpp_lt_proj_all.csv.dvc` — per cent
- `snapshots/un/2024-12-02/un_wpp_lt_proj_f.csv.dvc` — per cent
- `snapshots/un/2024-12-02/un_wpp_lt_proj_m.csv.dvc` — per cent
- `snapshots/un/2024-12-31/households.xlsx.dvc` — per cent
- `snapshots/un/2026-02-03/ilostat.parquet.dvc` — modelled
- `snapshots/un/2026-02-03/ilostat_dictionary_classif1.parquet.dvc` — modelled
- `snapshots/un/2026-02-03/ilostat_dictionary_classif2.parquet.dvc` — modelled
- `snapshots/un/2026-02-03/ilostat_dictionary_indicator.parquet.dvc` — modelled
- `snapshots/un/2026-02-03/ilostat_dictionary_note_classif.parquet.dvc` — modelled
- `snapshots/un/2026-02-03/ilostat_dictionary_note_indicator.parquet.dvc` — modelled
- `snapshots/un/2026-02-03/ilostat_dictionary_note_source.parquet.dvc` — modelled
- `snapshots/un/2026-02-03/ilostat_dictionary_obs_status.parquet.dvc` — modelled
- `snapshots/un/2026-02-03/ilostat_dictionary_ref_area.parquet.dvc` — modelled
- `snapshots/un/2026-02-03/ilostat_dictionary_sex.parquet.dvc` — modelled
- `snapshots/un/2026-02-03/ilostat_dictionary_source.parquet.dvc` — modelled
- `snapshots/un/2026-02-03/ilostat_extract.py` — modelled
- `snapshots/un/2026-02-03/ilostat_table_of_contents_country.parquet.dvc` — modelled
- `snapshots/un/2026-05-06/child_labor_england_wales.csv.dvc` — programmes
- `snapshots/un/2026-05-06/child_labor_japan.csv.dvc` — programmes

### `covid` — 42 files

- `etl/steps/data/garden/covid/latest/oxcgrt_policy.meta.yml` — prioritised
- `etl/steps/data/garden/covid/latest/sequence.meta.yml` — categorised
- `etl/steps/data/garden/covid/latest/xm_who.meta.yml` — modelled
- `snapshots/covid/latest/oxcgrt_policy_compact.csv.dvc` — organised
- `snapshots/covid/latest/oxcgrt_policy_national_2020.csv.dvc` — organised
- `snapshots/covid/latest/oxcgrt_policy_national_2021.csv.dvc` — organised
- `snapshots/covid/latest/oxcgrt_policy_national_2022.csv.dvc` — organised
- `snapshots/covid/latest/oxcgrt_policy_vaccines.csv.dvc` — organised
- `snapshots/covid/latest/vaccinations_global.csv.dvc` — programme
- `snapshots/covid/latest/xm_who.zip.dvc` — modelled
- `snapshots/covid/latest/yougov_australia.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_brazil.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_canada.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_china.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_composite.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_denmark.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_extra_mapping.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_finland.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_france.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_germany.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_hong_kong.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_india.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_indonesia.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_israel.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_italy.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_japan.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_malaysia.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_mexico.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_netherlands.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_norway.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_philippines.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_saudi_arabia.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_singapore.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_south_korea.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_spain.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_sweden.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_taiwan.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_thailand.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_united_arab_emirates.csv.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_united_kingdom.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_united_states.zip.dvc` — behaviour, behavioural, behaviours
- `snapshots/covid/latest/yougov_vietnam.csv.dvc` — behaviour, behavioural, behaviours

### `health` — 25 files

- `etl/steps/data/garden/health/2011/air_pollution_by_city__fouquet_and_dpcc__2011.meta.yml` — metre
- `etl/steps/data/garden/health/2018-04-17/alcohol_consumption_in_usa_since_1850__niaaa.meta.yml` — litres
- `etl/steps/data/garden/health/2023-04-18/shared.py` — neighbourhood
- `etl/steps/data/garden/health/2023-04-25/shared.py` — neighbourhood
- `etl/steps/data/garden/health/2023-04-25/wgm_2018.py` — harmonise
- `etl/steps/data/garden/health/2024-08-23/eurostat_cancer.meta.yml` — programmes
- `etl/steps/data/garden/health/2025-01-22/unaids.indicators_to_dimensions.yml` — programmes
- `etl/steps/data/garden/health/2025-12-15/unaids.dimensions.yml` — modelled
- `etl/steps/data/garden/health/2025-12-15/unaids.indicator_renames.yml` — programmes
- `etl/steps/data/garden/health/2025-12-15/unaids.indicators_to_dimensions.yml` — programmes
- `etl/steps/data/garden/health/2026-01-19/unaids.indicators_to_dimensions.yml` — programmes
- `etl/steps/data/garden/health/2026-01-19/unaids.meta.yml` — behaviour, behavioural, behaviours, centre, modelled, modelling, programme, programmes
- `etl/steps/data/garden/health/2026-01-19/unaids.py` — programme
- `snapshots/health/2017-07-20/prevalence_of_weight_categories_in_females__ncdrisc__2017.feather.dvc` — analysing
- `snapshots/health/2017-07-20/prevalence_of_weight_categories_in_males__ncdrisc__2017.feather.dvc` — analysing
- `snapshots/health/2019-09-18/annual_deaths_averted_by_pcv13__chen_et_al__the_lancet_global_health__2019.feather.dvc` — modelling
- `snapshots/health/2023-08-09/unaids.csv.dvc` — programme
- `snapshots/health/2023-08-11/unaids_hiv_children.xlsx.dvc` — programme
- `snapshots/health/2023-08-22/unaids_condom_msm.xlsx.dvc` — programme
- `snapshots/health/2023-08-22/unaids_deaths_averted_art.xlsx.dvc` — programme
- `snapshots/health/2023-08-22/unaids_gap_art.xlsx.dvc` — programme
- `snapshots/health/2024-08-23/eurostat_cancer.csv.dvc` — programme
- `snapshots/health/2026-01-19/unaids_epi.zip.dvc` — programme
- `snapshots/health/2026-01-19/unaids_gam.zip.dvc` — programmes
- `snapshots/health/2026-01-19/unaids_ncpi.zip.dvc` — analyse

### `oecd` — 23 files

- `etl/steps/data/garden/oecd/2018-01-01/developmental_food_aid__oecd__2018.meta.yml` — categorised, programme
- `etl/steps/data/garden/oecd/2018-03-11/road_deaths_and_injuries.meta.yml` — kilometres
- `etl/steps/data/garden/oecd/2019-05-04/corporate_income_tax__cit__corporate_tax_statistics_database__ctsd__oecd__2019.meta.yml` — analyse
- `etl/steps/data/garden/oecd/2023-09-21/plastic_use_polymer.meta.yml` — fibres
- `etl/steps/data/garden/oecd/2024-04-30/affordable_housing_database.meta.yml` — centres, organisations
- `etl/steps/data/garden/oecd/2024-07-01/road_accidents.meta.yml` — kilometre, travelled
- `etl/steps/data/garden/oecd/2024-12-30/family_database.meta.yml` — centre
- `etl/steps/data/garden/oecd/2025-12-11/social_expenditure.meta.yml` — programmes
- `etl/steps/data/garden/oecd/2026-07-07/health_expenditure.meta.yml` — categorising, organisation, organisations, programme, programmes
- `etl/steps/data/garden/oecd/2026-07-27/official_development_assistance.meta.yml` — programme, programmes
- `etl/steps/data/garden/oecd/2026-07-27/official_development_assistance.py` — programme, programmes
- `etl/steps/data/meadow/oecd/2025-12-11/social_expenditure.py` — programme
- `snapshots/oecd/2024-02-23/health_expenditure.csv.dvc` — organised
- `snapshots/oecd/2024-07-01/passenger_travel.csv.dvc` — kilometres
- `snapshots/oecd/2025-02-25/health_expenditure.csv.dvc` — organised
- `snapshots/oecd/2025-02-25/social_expenditure.csv.dvc` — analysing, programme, programmes
- `snapshots/oecd/2025-07-10/health_expenditure.csv.dvc` — organised
- `snapshots/oecd/2025-12-11/social_expenditure.csv.dvc` — analysing, programme, programmes
- `snapshots/oecd/2026-04-01/govt_glance_public_finance.csv.dvc` — analyse
- `snapshots/oecd/2026-04-01/govt_glance_public_finance_by_function.csv.dvc` — analyse
- `snapshots/oecd/2026-04-01/govt_glance_public_finance_economic_transaction.csv.dvc` — analyse
- `snapshots/oecd/2026-04-01/govt_glance_size_public_procurement.csv.dvc` — analyse
- `snapshots/oecd/2026-07-07/health_expenditure.csv.dvc` — organised

### `backport` — 15 files

- `snapshots/backport/latest/dataset_1015_mobile_bank_accounts_by_region__gsma__2019_config.json.dvc` — analysing, programme
- `snapshots/backport/latest/dataset_1015_mobile_bank_accounts_by_region__gsma__2019_values.feather.dvc` — analysing, programme
- `snapshots/backport/latest/dataset_1960_harmful_use_of_alcohol__defined_according_to_the_national_context_as_alcohol_per_capita_consumption__aged_15_years_and_older__within_a_calendar_year_in_litres_of_pure_alcohol_values.feather.dvc` — litres
- `snapshots/backport/latest/dataset_1979_growth_rates_of_household_expenditure_or_income_per_capita_among_the_bottom_40_per_cent_of_the_population_and_the_total_population_values.feather.dvc` — per cent
- `snapshots/backport/latest/dataset_1990_proportion_of_countries_that__a__have_conducted_at_least_one_population_and_housing_census_in_the_last_10_years__and__b__have_achieved_100_per_cent_birth_registration_and_80_per_cent_death_registration_values.feather.dvc` — per cent
- `snapshots/backport/latest/dataset_2839_global_data_set_on_education_quality__1965_2015__altinok__angrist__and_patrinos__2018_config.json.dvc` — favour
- `snapshots/backport/latest/dataset_2839_global_data_set_on_education_quality__1965_2015__altinok__angrist__and_patrinos__2018_values.feather.dvc` — favour
- `snapshots/backport/latest/dataset_3224_government_expenditure_and_learning_outcomes_config.json.dvc` — favour
- `snapshots/backport/latest/dataset_3224_government_expenditure_and_learning_outcomes_values.feather.dvc` — favour
- `snapshots/backport/latest/dataset_3226_average_harmonised_learning_outcome_score__2005_2015__altinok__angrist__and_patrinos__2018_config.json.dvc` — favour
- `snapshots/backport/latest/dataset_3226_average_harmonised_learning_outcome_score__2005_2015__altinok__angrist__and_patrinos__2018_values.feather.dvc` — favour
- `snapshots/backport/latest/dataset_4463_growth_rates_of_household_expenditure_or_income_per_capita_among_the_bottom_40_per_cent_of_the_population__pct_values.feather.dvc` — per cent
- `snapshots/backport/latest/dataset_4699_alcohol_consumption_per_capita__aged_15_years_and_older__within_a_calendar_year__litres_of_pure_alcohol_values.feather.dvc` — litres
- `snapshots/backport/latest/dataset_5033_investment__government_expenditure_config.json.dvc` — categorise
- `snapshots/backport/latest/dataset_5033_investment__government_expenditure_values.feather.dvc` — categorise

### `who` — 12 files

- `etl/steps/data/garden/who/2024-03-24/self_inflicted_injuries.py` — labelling
- `etl/steps/data/garden/who/2024-04-08/polio.meta.yml` — labelled
- `etl/steps/data/garden/who/2025-01-28/vaccine_safety.meta.yml` — programme
- `etl/steps/data/garden/who/2025-04-07/gho_smoking.meta.yml` — metres
- `etl/steps/data/garden/who/2025-04-17/mortality_database.py` — labelling
- `etl/steps/data/garden/who/2025-08-05/mortality_database_cancer.py` — labelling
- `etl/steps/data/garden/who/2025-08-06/mortality_database_vaccine_preventable.py` — labelling
- `etl/steps/data/garden/who/2025-08-06/self_inflicted_injuries.py` — labelling
- `etl/steps/data/garden/who/2026-05-22/gho.meta.yml` — analysed
- `snapshots/who/2022-09-30/ghe.feather.dvc` — programmes
- `snapshots/who/2024-07-30/ghe.feather.dvc` — programmes
- `snapshots/who/latest/avian_influenza_ah5n1.py` — labelled

### `fasttrack` — 11 files

- `etl/steps/data/grapher/fasttrack/2023-10-05/great_pacific_garbage_lebreton.meta.yml` — centimetres
- `etl/steps/data/grapher/fasttrack/latest/deforestation_by_commodity_singh.meta.yml` — fibres
- `etl/steps/data/grapher/fasttrack/latest/deforestation_commodity_singh_perrson.meta.yml` — fibres
- `etl/steps/data/grapher/fasttrack/latest/road_miles_uk.meta.yml` — travelled
- `etl/steps/data/grapher/fasttrack/latest/transport_co2_emissions_modes.meta.yml` — travelled
- `snapshots/fasttrack/latest/cumulative_lives_saved_vaccination_shattock.csv.dvc` — modelling
- `snapshots/fasttrack/latest/heat_deaths_zhao.csv.dvc` — modelling
- `snapshots/fasttrack/latest/infant_mortality_vaccination_shattock.csv.dvc` — modelling
- `snapshots/fasttrack/latest/lives_saved_measles_vaccination_who.csv.dvc` — modelling
- `snapshots/fasttrack/latest/lives_saved_vaccination_who.csv.dvc` — modelling
- `snapshots/fasttrack/latest/usa_hurricane_categories_noaa.csv.dvc` — categorised

### `wb` — 11 files

- `etl/steps/data/garden/wb/2022-10-03/world_bank_pip.meta.yml` — behaviour
- `etl/steps/data/garden/wb/2024-01-17/shared.py` — behaviour
- `etl/steps/data/garden/wb/2024-03-27/shared.py` — behaviour
- `etl/steps/data/garden/wb/2024-10-07/shared.py` — behaviour
- `etl/steps/data/garden/wb/2024-11-04/edstats.meta.yml` — enrolment
- `etl/steps/data/garden/wb/2025-04-08/shared.py` — behaviour
- `etl/steps/data/garden/wb/2025-06-05/shared.py` — behaviour
- `etl/steps/data/garden/wb/2025-08-07/shared.py` — behaviour
- `etl/steps/data/garden/wb/2025-09-08/gender_statistics.py` — modelled, organisation, programme
- `etl/steps/data/garden/wb/2025-10-09/shared.py` — behaviour
- `etl/steps/data/garden/wb/2026-06-26/shared.py` — behaviour

### `worldbank_wdi` — 11 files

- `etl/steps/data/garden/worldbank_wdi/2022-05-26/wdi/wdi.variable_mapping.json` — defence
- `etl/steps/data/garden/worldbank_wdi/2023-05-29/wdi.sources.json` — modelled
- `etl/steps/data/garden/worldbank_wdi/2024-05-20/wdi.sources.json` — modelled
- `etl/steps/data/garden/worldbank_wdi/2025-01-24/wdi.meta.override.yml` — enrolment
- `etl/steps/data/garden/worldbank_wdi/2025-01-24/wdi.sources.json` — modelled
- `etl/steps/data/garden/worldbank_wdi/2026-01-29/wdi.meta.override.yml` — enrolment, modelled
- `etl/steps/data/garden/worldbank_wdi/2026-01-29/wdi.sources.json` — modelled
- `etl/steps/data/garden/worldbank_wdi/2026-02-27/wdi.meta.override.yml` — enrolment, modelled
- `etl/steps/data/garden/worldbank_wdi/2026-02-27/wdi.sources.json` — modelled
- `etl/steps/data/garden/worldbank_wdi/2026-07-27/wdi.meta.override.yml` — enrolment, modelled
- `etl/steps/data/garden/worldbank_wdi/2026-07-27/wdi.sources.json` — modelled

### `multidim` — 10 files

- `etl/steps/export/multidim/education/latest/enrolment_rates.py` — enrolment
- `etl/steps/export/multidim/education/latest/years_of_schooling.py` — enrolment
- `etl/steps/export/multidim/ihme_gbd/latest/air_pollution.config.yml` — modelled
- `etl/steps/export/multidim/natural_disasters/latest/affected.py` — colour
- `etl/steps/export/multidim/natural_disasters/latest/deaths.py` — colour
- `etl/steps/export/multidim/natural_disasters/latest/economic_damages.py` — colour
- `etl/steps/export/multidim/natural_disasters/latest/shared.py` — colour
- `etl/steps/export/multidim/urbanization/latest/cities_towns_rural_areas.config.yml` — kilometre
- `etl/steps/export/multidim/urbanization/latest/cities_towns_rural_areas.py` — kilometre
- `etl/steps/export/multidim/worldbank_wdi/latest/ilostat_comparison.config.yml` — modelled

### `war` — 10 files

- `etl/steps/data/garden/war/2023-06-22/ucdp.py` — categorise, categorised, prioritise
- `etl/steps/data/garden/war/2023-09-21/ucdp.py` — categorise, categorised, prioritise
- `etl/steps/data/garden/war/2024-08-26/ucdp.py` — categorise, categorised, prioritise
- `etl/steps/data/garden/war/2024-10-02/ucdp_monthly.py` — categorise, categorised, prioritise
- `etl/steps/data/garden/war/2024-11-22/ucdp_preview.py` — categorise, categorised, prioritise
- `etl/steps/data/garden/war/2025-06-13/ucdp.py` — categorise, categorised, prioritise
- `etl/steps/data/garden/war/2026-06-10/ucdp.py` — categorise, categorised, prioritise
- `snapshots/war/2023-01-09/sorokin_1937.csv.dvc` — per cent
- `snapshots/war/2023-03-14/cow.py` — organised
- `snapshots/war/2023-09-21/cow.py` — organised

### `democracy` — 9 files

- `etl/steps/data/garden/democracy/2024-03-07/bmr.py` — behaviour, colour
- `etl/steps/data/garden/democracy/2024-03-07/bti.py` — labelled
- `etl/steps/data/garden/democracy/2024-03-07/shared.py` — behaviour, colour
- `etl/steps/data/garden/democracy/2025-03-05/shared.py` — behaviour, colour
- `etl/steps/data/garden/democracy/2026-03-27/bti.py` — labelled
- `etl/steps/data/garden/democracy/shared.py` — behaviour, colour
- `snapshots/democracy/2024-05-22/eiu_2021.csv.dvc` — travelling
- `snapshots/democracy/2024-05-22/eiu_2022.csv.dvc` — analysed
- `snapshots/democracy/2024-05-22/eiu_2023.csv.dvc` — analysed

### `faostat` — 9 files

- `etl/steps/data/garden/faostat/2022-05-17/shared.py` — prioritise
- `etl/steps/data/garden/faostat/2023-02-22/shared.py` — analysed, prioritise
- `etl/steps/data/garden/faostat/2023-06-12/shared.py` — analysed, prioritise
- `etl/steps/data/garden/faostat/2024-03-14/shared.py` — prioritise
- `etl/steps/data/garden/faostat/2025-03-17/shared.py` — fibre, fibres, prioritise
- `etl/steps/data/garden/faostat/2026-02-25/faostat_fbsc.py` — prioritising
- `etl/steps/data/garden/faostat/2026-02-25/shared.py` — fibre, fibres, prioritise
- `etl/steps/data/garden/faostat/2026-05-07/food_trade.items.yaml` — recognised
- `etl/steps/data/garden/faostat/2026-05-22/additional_variables.py` — fibre

### `unesco` — 8 files

- `etl/steps/data/garden/unesco/2025-05-01/education_opri.meta.yml` — favour, programmes
- `etl/steps/data/garden/unesco/2025-05-01/education_opri.py` — programmes
- `etl/steps/data/garden/unesco/2025-05-01/education_sdgs.meta.yml` — ageing, defence, modelled, programme, programmes
- `etl/steps/data/garden/unesco/2026-05-12/education_opri.meta.yml` — favour, programmes
- `etl/steps/data/garden/unesco/2026-05-12/education_opri.py` — enrolment, programmes
- `etl/steps/data/garden/unesco/2026-05-12/education_sdgs.meta.yml` — ageing, defence, modelled, programme, programmes
- `etl/steps/data/garden/unesco/2026-05-12/education_sdgs.py` — enrolment, modelled
- `snapshots/unesco/2024-11-21/enrolment_rates.csv.dvc` — enrolment

### `urbanization` — 8 files

- `etl/steps/data/garden/urbanization/2025-12-10/ghsl_countries.meta.yml` — kilometre
- `etl/steps/data/garden/urbanization/2025-12-10/ghsl_urban_centers.meta.yml` — kilometre, modelling
- `etl/steps/data/garden/urbanization/2025-12-10/ghsl_urban_centers.py` — centre
- `etl/steps/data/garden/urbanization/2026-03-02/sdg_11_2_1.meta.yml` — metres
- `etl/steps/data/meadow/urbanization/2025-12-10/ghsl_urban_centers.py` — centre, centres
- `snapshots/urbanization/2024-12-02/ghsl_urban_centers.xlsx.dvc` — centres
- `snapshots/urbanization/2025-12-10/ghsl_countries.xlsx.dvc` — harmonised
- `snapshots/urbanization/2025-12-10/ghsl_urban_centers.xlsx.dvc` — centres, harmonised

### `economics` — 7 files

- `etl/steps/data/garden/economics/2017/productivity__level_of_gdp_per_capita_and_productivity.meta.yml` — utilisation
- `etl/steps/data/garden/economics/2018-03-22/womens_economic_opportunity_2012__economist_intelligence_unit__2012.meta.yml` — favourable
- `etl/steps/data/garden/economics/2018-10-01/country_programmable_aid__cpa__from_2000_2019__oecd__2018.meta.yml` — programme
- `snapshots/economics/2016/eci_country_rankings__observatory_of_economic_complexity__2016__and_the_atlas_of_economic_complexity__2016.feather.dvc` — favourable
- `snapshots/economics/2017/productivity__level_of_gdp_per_capita_and_productivity.feather.dvc` — organisation
- `snapshots/economics/2018-03-22/womens_economic_opportunity_2012__economist_intelligence_unit__2012.feather.dvc` — favourable
- `snapshots/economics/2018-09-18/extreme_poverty_share_to_2030__crespo_cuaresma__2018.feather.dvc` — modelled

### `antibiotics` — 6 files

- `etl/steps/data/garden/antibiotics/2024-10-23/tracss.py` — analysing, centre
- `etl/steps/data/garden/antibiotics/2024-12-03/glass_enrolment.meta.yml` — enrolment
- `etl/steps/data/garden/antibiotics/2024-12-03/glass_enrolment.py` — enrolment
- `snapshots/antibiotics/2024-10-09/gram.csv.dvc` — modelling
- `snapshots/antibiotics/2024-10-09/gram_children.csv.dvc` — modelled, modelling
- `snapshots/antibiotics/2024-10-09/gram_level.csv.dvc` — modelling

### `agriculture` — 5 files

- `etl/steps/data/garden/agriculture/2026-03-02/attainable_yields.meta.yml` — fibre, fibres
- `etl/steps/data/garden/agriculture/2026-03-02/long_term_crop_yields.meta.yml` — fibre, fibres
- `etl/steps/data/garden/agriculture/2026-07-08/livestock_counts.py` — favour
- `snapshots/agriculture/2017-08-10/projections_of_peak_agricultural_land__fao__2006__oecd__2012__mea__2005.feather.dvc` — modelling
- `snapshots/agriculture/2017/water_withdrawals_and_consumption__aquastat.feather.dvc` — millimetres

### `biodiversity` — 5 files

- `etl/steps/data/garden/biodiversity/2024-10-30/fish_stocks.py` — harmonisation
- `snapshots/biodiversity/2022/living_planet_index.feather.dvc` — modelling
- `snapshots/biodiversity/2024-09-30/living_planet_index.xlsx.dvc` — modelling
- `snapshots/biodiversity/2024-09-30/living_planet_index_completeness.csv.dvc` — modelling
- `snapshots/biodiversity/2024-09-30/living_planet_index_share.csv.dvc` — modelling

### `climate` — 5 files

- `etl/steps/data/garden/climate/2026-07-10/total_precipitation.meta.yml` — metres
- `etl/steps/data/grapher/climate/2026-07-10/sst_by_month.meta.yml` — colour
- `etl/steps/data/grapher/climate/2026-07-10/total_precipitation_annual.meta.yml` — metres
- `snapshots/climate/2018-04-06/ozone_depletion_impacts_on_skin_cancer_incidence__slaper_et_al__1996.feather.dvc` — modelled
- `snapshots/climate/2023-12-20/surface_temperature.gz.dvc` — licence

### `demography` — 5 files

- `etl/steps/data/garden/demography/2022-12-08/population/__init__.py` — prioritised
- `etl/steps/data/garden/demography/2023-03-31/population/__init__.py` — prioritised
- `snapshots/demography/2018-03-21/historical_gender_equality_index__how_was_life__2014.feather.dvc` — favour
- `snapshots/demography/2020-10-01/labour_force_dependency_ratio__iiasc.feather.dvc` — modelling
- `snapshots/demography/2021/share_of_one_person_households__owid_based_on_un_and_other_sources.feather.dvc` — favoured

### `emissions` — 5 files

- `etl/steps/data/garden/emissions/2017/emissions_air_pollutants_over_long_term__defra__and__epa.meta.yml` — sulphur_dioxide__index, sulphur_dioxide__so2
- `etl/steps/data/garden/emissions/2025-12-04/national_contributions.meta.yml` — sulphur
- `etl/steps/data/garden/emissions/2026-07-13/net_zero_tracker.meta.yml` — labelled
- `etl/steps/data/garden/emissions/2026-07-13/net_zero_tracker.py` — labelled
- `etl/steps/data/grapher/emissions/2026-07-13/net_zero_tracker.py` — colour, colours

### `itopf` — 5 files

- `etl/steps/data/garden/itopf/2025-05-05/oil_spills.meta.yml` — categorised
- `snapshots/itopf/2023-05-18/oil_spills.pdf.dvc` — categorised
- `snapshots/itopf/2024-10-16/oil_spills.pdf.dvc` — categorised
- `snapshots/itopf/2025-05-05/oil_spills.pdf.dvc` — categorised
- `snapshots/itopf/2026-05-06/oil_spills.pdf.dvc` — categorised

### `animal_welfare` — 4 files

- `snapshots/animal_welfare/2023-08-01/uk_egg_statistics.ods.dvc` — per cent
- `snapshots/animal_welfare/2023-08-08/farmed_finfishes_used_for_food.zip.dvc` — recognised
- `snapshots/animal_welfare/2024-12-12/farmed_finfishes_used_for_food.zip.dvc` — recognised
- `snapshots/animal_welfare/2026-04-16/uk_egg_statistics.ods.dvc` — per cent

### `artificial_intelligence` — 4 files

- `etl/steps/data/garden/artificial_intelligence/2026-06-08/nvidia_revenue.py` — harmonises
- `etl/steps/data/meadow/artificial_intelligence/2026-04-27/cset.py` — labelled
- `snapshots/artificial_intelligence/2023-06-14/ai_adoption.csv.dvc` — organisations
- `snapshots/artificial_intelligence/2026-06-08/nvidia_revenue.py` — harmonisation, labelled

### `education` — 4 files

- `etl/steps/data/garden/education/2023-07-17/education_lee_lee.meta.yml` — enrolment
- `etl/steps/data/garden/education/2025-08-20/harmonized_scores.meta.yml` — programmes
- `snapshots/education/2017-09-30/public_expenditure.feather.dvc` — prioritised
- `snapshots/education/2018-02-20/unesco_metadata_on_literacy__uis__2017.feather.dvc` — categorised

### `ember` — 4 files

- `etl/steps/data/garden/ember/2022-08-01/combined_electricity_review.py` — prioritise
- `etl/steps/data/garden/ember/2022-08-01/global_electricity_review.py` — harmonising
- `etl/steps/data/garden/ember/2023-02-20/shared.py` — fulfil
- `etl/steps/data/garden/ember/2023-07-10/shared.py` — fulfil

### `papers` — 4 files

- `etl/steps/data/garden/papers/2024-03-26/broadberry_et_al_2015.py` — litre, litres
- `etl/steps/data/garden/papers/2026-01-20/anshassi_waste_management.meta.yml` — organised
- `snapshots/papers/2023-05-26/ray_et_al_2019.xlsx.dvc` — modelled
- `snapshots/papers/2024-03-26/ray_et_al_2019.xlsx.dvc` — modelled

### `plastic_waste` — 4 files

- `etl/steps/data/garden/plastic_waste/2026-01-14/cottom_plastic_waste.meta.yml` — millimetres, modelled
- `snapshots/plastic_waste/2018-01-01/plastic_product_lifetime__production__waste_by_source__geyer_et_al__2017.feather.dvc` — fibres
- `snapshots/plastic_waste/2018-01-01/surface_ocean_plastic_by_mass__eriksen_et_al__2014.feather.dvc` — modelling
- `snapshots/plastic_waste/2018-01-01/surface_ocean_plastic_by_particle_count__eriksen_et_al__2014.feather.dvc` — modelling

### `cancer` — 3 files

- `etl/steps/data/garden/cancer/2024-09-13/diagnosis_routes_by_route.meta.yml` — programmes
- `snapshots/cancer/2024-09-13/diagnosis_routes_by_route.csv.dvc` — programmes
- `snapshots/cancer/2024-09-13/diagnosis_routes_by_stage.csv.dvc` — programmes

### `core_econ` — 3 files

- `etl/steps/data/garden/core_econ/2020-07-24/te_17_4.meta.yml` — organisation
- `etl/steps/data/garden/core_econ/2020-07-28/te_14_1_gdp.meta.yml` — per cent
- `etl/steps/data/garden/core_econ/2020-07-28/te_14_1_gov.meta.yml` — per cent

### `emdat` — 3 files

- `etl/steps/data/garden/emdat/2023-09-20/shared.py` — fulfil
- `etl/steps/data/garden/emdat/2026-04-30/natural_disasters.meta.yml` — modelling
- `etl/steps/data/garden/emdat/2026-04-30/natural_disasters.py` — fulfil

### `energy_institute` — 3 files

- `etl/steps/data/garden/energy_institute/2026-06-30/statistical_review_of_world_energy.meta.yml` — metres
- `etl/steps/data/meadow/energy_institute/2025-06-27/statistical_review_of_world_energy.py` — metres
- `etl/steps/data/meadow/energy_institute/2026-06-30/statistical_review_of_world_energy.py` — metres

### `geography` — 3 files

- `etl/steps/data/garden/geography/2018-04-15/terrain_ruggedness_index__nunn_and_puga__2012.meta.yml` — metres
- `etl/steps/data/garden/geography/2026-01-27/neighbours.meta.yml` — neighbours
- `etl/steps/data/garden/geography/2026-01-27/neighbours.py` — neighbour, neighbouring, neighbours

### `ihme_gbd` — 3 files

- `etl/steps/data/garden/ihme_gbd/2019/shared.py` — labelling
- `etl/steps/data/garden/ihme_gbd/2026-02-07/gbd_cause_deaths.py` — labelling
- `etl/steps/data/garden/ihme_gbd/2026-02-11/gbd_cancers_deaths.py` — labelling

### `tuberculosis` — 3 files

- `etl/steps/data/garden/tuberculosis/2026-02-05/budget.meta.yml` — programme
- `etl/steps/data/garden/tuberculosis/2026-02-05/burden_estimates.meta.yml` — programmes
- `etl/steps/data/garden/tuberculosis/2026-02-05/expenditure.meta.yml` — programme

### `ahdi` — 2 files

- `snapshots/ahdi/2023-09-08/augmented_hdi.xlsx.dvc` — per cent
- `snapshots/ahdi/2023-09-08/augmented_hdi_region.xlsx.dvc` — per cent

### `aviation` — 2 files

- `etl/steps/data/garden/aviation/2026-04-02/air_traffic.meta.yml` — kilometre, kilometres
- `etl/steps/data/garden/aviation/2026-04-02/air_traffic.py` — kilometres

### `chartbook` — 2 files

- `snapshots/chartbook/2024-08-15/jantti_2010_finland.csv.dvc` — per cent
- `snapshots/chartbook/2024-08-19/riihela_et_al_2003_finland.csv.dvc` — analyse

### `energy` — 2 files

- `etl/steps/data/garden/energy/2022-08-03/shared.py` — prioritise, prioritising
- `etl/steps/data/garden/energy/2025-11-10/ireland_metered_consumption.meta.yml` — analysed

### `excess_mortality` — 2 files

- `etl/steps/data/garden/excess_mortality/latest/wmd.py` — harmonising
- `etl/steps/data/garden/excess_mortality/latest/xm_karlinsky_kobak.py` — harmonising

### `explorers` — 2 files

- `etl/steps/export/explorers/faostat/latest/global_food.py` — fibre
- `etl/steps/export/explorers/war/latest/conflict_data_source.py` — labelled

### `forests` — 2 files

- `etl/steps/data/garden/forests/2025-05-08/forest_share.meta.yml` — metres
- `etl/steps/data/garden/forests/2025-12-29/forest_share.meta.yml` — metres

### `gcp` — 2 files

- `etl/steps/data/garden/gcp/2025-11-13/global_carbon_budget.meta.yml` — colour
- `snapshots/gcp/2023-09-28/global_carbon_budget.py` — organisations, recognised

### `ilostat` — 2 files

- `snapshots/ilostat/2023-09-19/employment.csv.dvc` — modelled
- `snapshots/ilostat/2023-09-19/unemployment.csv.dvc` — modelled

### `news` — 2 files

- `snapshots/news/2024-05-07/country_tags.yaml` — behaviour, centre, defence, programme
- `snapshots/news/2025-10-29/guardian_mentions.py` — organisations

### `ons` — 2 files

- `etl/steps/data/grapher/ons/2025-10-07/divorces.meta.yml` — recognised
- `etl/steps/data/grapher/ons/2025-10-07/divorces_by_year.meta.yml` — centres

### `static_viz` — 2 files

- `etl/steps/export/static_viz/population/2026-02-03/pop_doubling.py` — colours
- `etl/steps/export/static_viz/who/2026-08-07/height_for_age.py` — centre, centres, colour, labelled, millimetres, recognises

### `statins` — 2 files

- `snapshots/statins/2023-10-05/bmj_2022.csv.dvc` — utilisation
- `snapshots/statins/2023-10-05/bmj_2022.py` — utilisation

### `unep` — 2 files

- `snapshots/unep/2023-01-03/global_trends_in_renewable_energy_investment.pdf.dvc` — centre
- `snapshots/unep/2023-12-12/global_trends_in_renewable_energy_investment.pdf.dvc` — centre

### `unicef` — 2 files

- `etl/steps/data/garden/unicef/2024-07-30/child_migration.meta.yml` — programmes
- `etl/steps/data/garden/unicef/2026-01-07/child_migration.meta.yml` — programmes

### `wash` — 2 files

- `etl/steps/data/garden/wash/2024-01-04/nutrients.meta.yml` — litre, metres
- `etl/steps/data/garden/wash/2025-12-08/household.meta.yml` — neighbour

### `working_hours` — 2 files

- `etl/steps/data/garden/working_hours/2018-06-28/new_estimates_of_hours_of_work_per_week__1900_1957__jones__1963.meta.yml` — travelling
- `snapshots/working_hours/2018-06-28/time_spent__participation_time__and_participation_rates__eurostat.feather.dvc` — centres

### `bgs` — 1 files

- `etl/steps/data/garden/bgs/2025-12-15/world_mineral_statistics.py` — fibre, metres

### `clio_infra` — 1 files

- `snapshots/clio_infra/2017-09-09/clio_infra__human_capital.feather.dvc` — programmes

### `countries` — 1 files

- `snapshots/countries/2025-03-22/continents_oceans.zip.dvc` — neighbour

### `crime` — 1 files

- `snapshots/crime/2026-01-20/prison_rates.csv.dvc` — organisations, programme

### `eia` — 1 files

- `etl/steps/data/garden/eia/2026-05-05/international_energy.py` — metres

### `ess` — 1 files

- `snapshots/ess/2023-08-02/ess_trust.csv.dvc` — behaviour

### `food` — 1 files

- `snapshots/food/2018/food_miles_by_transport_method__poore_and_nemecek__2018.feather.dvc` — kilometre

### `hmd` — 1 files

- `snapshots/hmd/2022-12-07/hmd.zip.dvc` — labelled

### `iea` — 1 files

- `snapshots/iea/2024-07-04/critical_minerals.xlsx.dvc` — behaviours

### `igh` — 1 files

- `etl/steps/data/garden/igh/2024-07-05/better_data_homelessness.meta.yml` — centres

### `ivs` — 1 files

- `etl/steps/data/garden/ivs/2025-06-27/integrated_values_surveys.meta.yml` — defence, organisations

### `labor` — 1 files

- `etl/steps/data/garden/labor/2026-02-17/female_labor_force_participation_omm.meta.yml` — modelled

### `lgbt_rights` — 1 files

- `etl/steps/data/garden/lgbt_rights/2026-05-11/lgbti_national_policy_dataset.py` — labelled

### `neglected_tropical_diseases` — 1 files

- `snapshots/neglected_tropical_diseases/2026-06-25/funding.xlsx.dvc` — prioritised

### `other` — 1 files

- `etl/steps/__init__.py` — labelled

### `owid_grapher` — 1 files

- `etl/steps/data/external/owid_grapher/latest/neighbours.py` — neighbour, neighbours

### `pew` — 1 files

- `etl/steps/data/garden/pew/2025-10-31/religious_composition.meta.yml` — categorised

### `plastic_pollution` — 1 files

- `snapshots/plastic_pollution/2019/missing_plastic_budget__lebreton_et_al__2019.feather.dvc` — modelled

### `poverty_inequality` — 1 files

- `snapshots/poverty_inequality/2018-09-17/extreme_poverty_2030_projections_by_ssp__crespo_et_al__2018.feather.dvc` — modelled

### `sipri` — 1 files

- `etl/steps/data/garden/sipri/2026-04-27/military_expenditure.meta.yml` — defence

### `survey` — 1 files

- `snapshots/survey/2023-08-07/afrobarometer_trust.csv.dvc` — analyse

### `technology` — 1 files

- `etl/steps/data/garden/technology/2004/technology_diffusion__comin_and_hobijn__2004__and_others.meta.yml` — colour

### `tourism` — 1 files

- `etl/steps/data/garden/tourism/2026-01-21/unwto.meta.yml` — travelling

### `transport` — 1 files

- `etl/steps/data/garden/transport/2017-07-11/non_commercial_flight_distance_records__wikipedia.meta.yml` — kilometres

### `tsmc` — 1 files

- `etl/steps/data/garden/tsmc/2025-11-10/operating_data.meta.yml` — labelled

### `uk_beis` — 1 files

- `snapshots/uk_beis/2023-07-10/uk_historical_electricity.xls.dvc` — licence

### `unece` — 1 files

- `etl/steps/data/garden/unece/2026-05-04/life_cycle_assessment_of_electricity.meta.yml` — metres

### `unodc` — 1 files

- `etl/steps/data/garden/unodc/2026-06-12/homicide.meta.yml` — defence

### `unu_wider` — 1 files

- `etl/steps/data/garden/unu_wider/2026-03-24/government_revenue_dataset.meta.yml` — organisations

### `waste` — 1 files

- `snapshots/waste/2018-03-16/environment__municipal_waste__generation_and_treatment.feather.dvc` — harmonised, organisation, organisations