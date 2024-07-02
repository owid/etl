---
tags:
  - 👷 Staff
---

# Update data

This guide explains the general workflow to update a dataset that already exists in ETL.
For simplicity, let's go through it with a real example: Assume you have to update the "Near-surface temperature anomaly" dataset, by the Met Office Hadley Centre.

This guide assumes you have already a working installation of `etl`, and use VSCode with the appropriate configuration and plugins.

These are the steps to follow:

## 1. Duplicate the old steps and set up your staging server
Firstly, you will create the "reference" code, which is the code that the final pull request (PR) be compared against.
This is not strictly necessary, but it will be very helpful for the PR reviewer.

- Go to ETL `master` branch, and ensure it's up-to-date in your local repository (by running `git pull`).
    - Ensure that, in your `.env` file, you have set `STAGING=1`. For more details on the `.env` file see this guide***.
- Create a "reference" branch (a temporary branch that will be convenient for the reviewer later on):
```bash
etl d draft-pr temp-update-temperature-anomaly
```
This will create a new git branch in your local repository, which will be pushed.
It will also create a draft pull request in github, and a staging server.
All these things are temporary (which is why we added the `temp-` in the name of the branch).
In a few moments you will know the reason why.
- Wait a few minutes, until you receive an email from `owidbot` saying that the staging server `http://staging-site-temp-update-temperature-anomaly/admin/login` has been created.
- Then, run your local ETL Wizard:
```bash
etlwiz
```
And click on "Dashboard".
- On the Steps table, select the grapher dataset you want to update. Click on "Add selected steps to the Operations list".
In this case, it has only 1 chart, so it will be an easy update.
    - Scroll down to the Operations list, and click on "Add all dependencies".
    - Click on "Remove non-updateable (e.g. population)" (although, for this simple example, it makes no difference).
    - Scroll down and expand the "Additional parameters to update steps" box, to deactivate the "Dry run" option.
    - Then click on "Update X steps" (in this case, X equals 6).
This will create all the new ETL code files needed for the update, and write those steps in the dag (in this case, in the `climate.yml` dag file).
You can close the Wizard (and kill it with `ctrl+c`).
- Commit those new files in `snapshots` and `etl` folders to your branch:
```bash
git add etl
git add snapshots
git commit -m "Duplicate previous Met Office steps"
```
NOTE: For convenience, do not commit the changes in the `dag`. In a few moments you will see why.
- Create a "review" branch (the branch that will be reviewed):
```bash
etl d draft-pr update-temperature-anomaly --base-branch temp-update-temperature-anomaly --title "Update Near-surface temperature anomaly data" --category data
```
This will create a sub-branch in your local repository, which will be pushed.
It will also create a draft pull request in github, and a staging server.
- Wait a few minutes, until you receive an email from `owidbot` saying that the staging server `http://staging-site-update-temperature-anomaly/admin/login` has been created.

## 2. Update and run the new steps
- Edit the snapshot metadata files in VSCode, if any modifications are needed (for example, the `date_published` field may need to be manually updated).
    - For convenience (throughout the rest of the work), open the corresponding dag file in a tab (`cmd+p` to open the Quick Open bar, then type `climate.yml` and `enter`).
    - To open a specific snapshot, go to the bottom of the dag, where the new steps are. Select the dag entry of one of the snapshots (without including the `snapshot://`), namely `met_office_hadley_centre/2024-07-02/near_surface_temperature_global.csv`, and then hit `cmd+c`, `cmd+p`, `cmd+v`, `enter`.
- Execute the snapshot:
```bash
python snapshots/met_office_hadley_centre/2024-07-02/near_surface_temperature.py
```
- In a similar fashion, edit the `meadow`, `garden`, and `grapher` steps, if needed, and execute them. You can do that either one by one:
```bash
etlr meadow/met_office_hadley_centre/2024-07-02/near_surface_temperature
etlr garden/met_office_hadley_centre/2024-07-02/near_surface_temperature
etlr grapher/met_office_hadley_centre/2024-07-02/near_surface_temperature
etlr grapher/met_office_hadley_centre/2024-07-02/near_surface_temperature --grapher
```
Or all at once:
```bash
etlr near_surface_temperature --grapher
```
NOTE: The ETL code is run locally, but the database you are accessing is the one from the staging server.
- Commit your changes to the branch (now you should also include the changes in the dag).
```bash
git add .
git commit -m "Update snapshots and data steps"
git push origin update-temperature-anomaly
```

## 3. Upgrade indicators used in charts
- Open the ETL wizard again:
```bash
etlwiz
```
And click on "Indicator Upgrader".
- By default, you should see selected the new grapher dataset (which has no charts), and its corresponding old version (with one chart). Press "Next".
- Ensure the mapping from old to new indicators is correct. Press "Next".
- Ensure the list of affected charts is as expected. Press "Update charts".

## 4. Approve chart differences
- Open Chart Diff.
- TODO: Continue here

