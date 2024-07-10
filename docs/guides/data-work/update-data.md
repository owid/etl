---
tags:
  - 👷 Staff
---

# Update data

This guide explains the general workflow to update a dataset that already exists in ETL.

!!! tip "Quick summary guide"

    In a nutshell, these are the steps to follow:

    - Create a _reference git branch_.
    - Use the ETL Dashboard to create new versions of the steps (by duplicating the old ones).
    - Commit the new files (without any further changes, and without committing changes to the dag) and push them to the _reference_ branch.
    - Create a _review_ branch (which is a sub-branch of the _reference_ branch) and a draft pull request (PR), which will also create a staging server where your chart work will take place.
    - Adapt the code of the new steps and ensure ETL (e.g. `etlr step-names --grapher`) can execute them successfully.
    - Commit changes to the code to the _review_ branch.
    - Use Indicator Upgrader to update the charts (so they use the new variables instead of the old ones).
        - If needed, adapt existing charts or create new ones on the staging server.
    - Archive old steps (i.e. move old steps from the dag to the archive dag).
    - Commit all your final work to the _review_ branch, and set your PR (merging _review_ to _reference_) to be ready for review.
        - Make further changes, if suggested by the reviewer.
    - Once approved, edit your PR, so that it merges _review_ to `master`, and merge the PR.
    - Archive old grapher dataset(s).
    - Announce your update.

For simplicity, let's go through it with a real example: Assume you have to update the "Near-surface temperature anomaly" dataset, by the Met Office Hadley Centre.

This guide assumes you have already a [working installation of `etl`](../../../getting-started/working-environment/), and use VSCode with the appropriate configuration and plugins.

## 1. Duplicate the old steps and set up your staging server

Firstly, you will create the _reference_ code, which is the code that the final pull request (PR) will be compared against.
This is not strictly necessary, but it will be very helpful for the PR reviewer. The need for this step will be clearer later on.

- **Update your `master` and configuration**:
    - Go to ETL `master` branch, and ensure it's up-to-date in your local repository (by running `git pull`).
    - Ensure that, in your `.env` file, you have set `STAGING=1`.
- **Create a _reference branch_**: That is a temporary branch that will be convenient for the reviewer later on.

    ```bash
    etl d draft-pr temp-update-temperature-anomaly
    ```

    This will create a new git branch in your local repository, which will be pushed.
    It will also create a draft pull request in github, and a staging server.
    All these things are temporary (which is why we added the `temp-` in the name of the branch).
    Wait for a notification from `owidbot`. It should take a few minutes, and will inform you that the staging server [http://staging-site-temp-update-temperature-anomaly/admin](http://staging-site-temp-update-temperature-anomaly/admin) has been created.

- **Update steps using the ETL Dashboard**:
    - Start the ETL Wizard, by running:
    ```bash
    etlwiz
    ```
    !!! note
        Even though it is possible to access the wizard from production or from a staging server, we recommend always using your local wizard. This means initialized from your local computer (but connecting to a staging database, with `STAGING=1`).
    - Inside the Wizard, go to "Dashboard".
    - On the Steps table, select the grapher dataset you want to update. Click on "Add selected steps to the Operations list".
    In this case, it has only 1 chart, so it will be an easy update.
    - Scroll down to the Operations list, and click on "Add all dependencies".
    - Click on "Remove non-updateable (e.g. population)" (although, for this simple example, it makes no difference).
    - Scroll down and expand the "Additional parameters to update steps" box, to deactivate the "Dry run" option.
    - Then click on "Update X steps" (in this case, X equals 6).
        This will create all the new ETL code files needed for the update, and write those steps in the dag (in this case, in the `climate.yml` dag file).
    <figure markdown="span">
    ![Chart Upgrader](../../../assets/etl-dashboard-update-steps.gif)
    <figcaption>Animation of how to update steps in ETL Dashboard.</figcaption>
    </figure>
    - You can close the Wizard (kill it with ++ctrl+c++).

- **Commit your changes**: Commit those new files in `snapshots` and `etl` folders to your branch:

    ```bash
    git add etl
    git add snapshots
    git commit -m "Duplicate previous Met Office steps"
    ```

    !!! note
        For convenience, do not commit the changes in the `dag` (more on this later).

- **Create a _review branch_**: This is the branch that will be reviewed.

    ```bash
    etl d draft-pr update-temperature-anomaly --base-branch temp-update-temperature-anomaly --title "Update Near-surface temperature anomaly data" --category data
    ```

    This will create a sub-branch in your local repository, which will be pushed.
    It will also create a draft pull request in github, and a staging server.
    Wait for a notification from `owidbot`. It should take few minutes, and will inform you that the staging server [http://staging-site-update-temperature-anomaly/admin](http://staging-site-update-temperature-anomaly/admin) has been created.

## 2. Update and run the new steps

So far we have prepared the working environment for the update. Now, you'll be adding new ETL steps, editing scripts, etc.

- **Edit the snapshot metadata files**: Some modifications may be needed, for example, the `date_published` field may need to be manually updated.

    - For convenience (throughout the rest of the work), open the corresponding dag file in a tab (++cmd+p++ to open the Quick Open bar, then type `climate.yml` and `enter`).
    - To open a specific snapshot, go to the bottom of the dag, where the new steps are. Select the dag entry of one of the snapshots (without including the `snapshot://` prefix), namely `met_office_hadley_centre/2024-07-02/near_surface_temperature_global.csv`, and then hit ++cmd+c++, ++cmd+p++, ++cmd+v++, ++enter++.

    <figure markdown="span">
        ![Chart Upgrader](../../../assets/etl-dag-open-file.gif)
        <figcaption>Animation of the editing process of a snapshot.</figcaption>
    </figure>

    !!! note
        We should always quickly have a look at the license URL, to ensure it has not changed (see [our guide on source's licenses](https://www.notion.so/owid/How-to-check-a-source-s-license-ade23e5e1e0f4610b98598f9d459f96e)).

- **Run the snapshot step**:

    ```bash
    python snapshots/met_office_hadley_centre/2024-07-02/near_surface_temperature.py
    ```

- **Run the `meadow`, `garden`, and `grapher` steps**: Edit these steps and execute them. You can do that either one by one:

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

    !!! note
        Remember that, even though your ETL code is run locally, the database you are accessing is the one from the staging server (because of the `STAGING=1` parameter in your `.env` file).

- **Commit your changes to the review branch**: You should now include the changes in the dag too.

    ```bash
    git add .
    git commit -m "Update snapshots and data steps"
    git push origin update-temperature-anomaly
    ```

## 3. Upgrade indicators used in charts

After updating the data, it is time to update the affected charts! This involves migrating the indicators used in some charts to the new ones available.

- **Update indicators using Indicator Upgrader**:
    - Start Wizard:
        ```bash
        etlwiz
        ```

        And click on _Indicator Upgrader_.

    - By default, you should see selected the new grapher dataset (which has no charts), and its corresponding old version (with one chart). Press _Next_.
    - Ensure the mapping from old to new indicators is correct. Press _Next_.
    - Ensure the list of affected charts is as expected. Press _Update charts_. This will update all the affected charts in the PR staging server.
    - If you have more datasets to update, simply refresh the page (++cmd+r++) and, by default, the next new dataset will be selected.

    <figure markdown="span">
        ![Chart Upgrader](../../../assets/indicator-upgrader-short.gif)
        <figcaption>Indicator upgrade flow.</figcaption>
    </figure>

- **Do further chart changes**: You can make any further changes to charts in your staging server, if needed.

    !!! note
        You should be making changes to charts in the _review_ server (namely [http://staging-site-update-temperature-anomaly/](http://staging-site-update-temperature-anomaly/)), and **not** in the _reference_ server (namely [http://staging-site-temp-update-temperature-anomaly/](http://staging-site-temp-update-temperature-anomaly/)).

## 4. Approve chart differences

Review all changes in charts.

- **Start Chart Diff in Wizard**: A link will appear at the bottom of the page when you've submitted the changes in the Indicator Upgrader. Alternatively, you can select it on the Wizard menu on the sidebar.
- **Review the chart changes**:
    - Inspect the changes in the charts, and approve them if everything looks good.
    - If you notice some issues, you can go back to the code and do further changes.
    - If you are not happy with the changes, you can reject these.

    <figure markdown="span">
        ![Chart Upgrader](../../../assets/chart-diff-short.gif)
        <figcaption>Chart diff flow. You'll be shown any chart that you've changed in your staging server (either via indicator upgrader or manually in the admin) compared to production. Here, you need to approve and/or reject the differences.</figcaption>
    </figure>

## 5. Archive unused steps

After your updates, the old steps are no longer relevant. Therefore, we move these to the archive dag. By doing this, we minimize the risk of using outdated steps by mistake.

- **Archive old steps using the ETL Dashboard**:
    - Go to ETL Dashboard in your local Wizard.
    - On the Steps table, select the old step (the one that you have just updated, and that now should appear as "Archivable"), and click on "Add selected steps to the Operations list".
    - Scroll down to the Operations list, and click on "Add all dependencies".
    - Scroll down and expand the "Additional parameters to archive steps" box, to deactivate the "Dry run" option.
    - Then click on "Archive X steps" (in this case, X equals 6).

    <figure markdown="span">
        ![Chart Upgrader](../../../assets/etl-dashboard-archive-steps.gif)
        <figcaption>Archive ETL steps.</figcaption>
    </figure>

- **Sanity-check your archived steps**: To ensure nothing has been archived by mistake, you can run `etl d version-tracker`.
- **Commit the changes in the dag files**.

## 6. Get your pull request reviewed

You have now completed the first iteration of your work. Time to get a second opinion on your changes!

!!! note "The PR to review is merging the _review_ branch into the _reference_ branch"
    Your current draft PR (called "📊 Update Near-surface temperature anomaly data") attempts to merge the sub-branch `update-temperature-anomaly` into `temp-update-temperature-anomaly`.

    We do it this way so that the reviewer will see how the code has changed with respect to its previous version.

    Otherwise, if the PR was comparing your branch with `master`, the reviewer would need to see all the code (that was already reviewed in the past) as if the steps were new.



- **Ensure CI/CD checks have passed**: In the GitHub page of the draft PR, check that all checks have a green tick.
    - If any of them has a red cross ❌:
        - Click on _Details_, to open Buildkite and get more details on the error(s).
        - Sometimes, retrying the check that failed fixes the problem. You can do this by clicking on the job that failed, and then clicking on _Retry_. If this does not solve the issue, ask for support.
- **Set the PR ready for review**: If you see that "All checks have passed", the PR is ready for review.
    - Add a meaningful description, stating all the main changes you made, possibly linking to any relevant GitHub issues.
    - Click on _Ready for review_.
    - Finally, add a reviewer. If the PR is very long and you want to have multiple reviewers, specify in the description what each one should review.
- **Implement changes**: Wait for the review, and implement any changes brought up by the reviewer that you consider apply.

## 7. Publish your work

Share the result of your work with the world.

- Once the PR is approved, click on "Edit" on the right of the PR title. You will see a dropdown to select the "base" of the PR. Change it to `master`, and confirm.
- Click on "Squash and merge" and confirm.
     - After this, the code for the new steps will be integrated with `master`. ETL will build the new steps in production, and, under the hood, all changes you made to charts on your staging server will be synced with public charts.

## 8. Archive old grapher datasets

For convenience, we should archive grapher datasets that have been replaced by new ones.

!!! note
    This step is a bit cumbersome, feel free to skip if you don't feel confident about it. There is [an open issue](https://github.com/owid/owid-grapher/issues/3308) to make this easier.

- Go to [the grapher dataset admin](https://admin.owid.io/admin/datasets).
- Search for the dataset (type "Near-surface"). Click on it.
- Copy the dataset id from the URL (e.g. if the URL is [https://admin.owid.io/admin/datasets/6520](https://admin.owid.io/admin/datasets/6520), the dataset id is `6520`).
- Access the production database (e.g. using DBeaver), search for the dataset with that id, and set `isPrivate` and `isArchived` to 1.

## 9. Wrap up

- Close any relevant issues from the `owid-issues` or `etl` repositories.
- Have a look at some of the public charts (like [the chart on temperature anomaly](https://ourworldindata.org/grapher/temperature-anomaly)) and their metadata.
- If your changes affect explorers, you can run `etl explorer-update`.
    - It may take a few minutes, and it will update all `*-explorer.tsv` files in your `owid-content` repository.
    - You can access the `owid-content` repository, and commit any useful changes (otherwise, you can revert them with `git restore .`).
    - Push those changes and create a new PR in `owid-content`.
- If it's an important update, announce it on slack `#article-and-data-updates` channel.
