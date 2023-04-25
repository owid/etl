An empty step used only to mark a dependency on a Github repo, and trigger a rebuild of later steps whenever that repo changes. This is useful since Github is a good store for data that updates too frequently to be snapshotted into Walden, e.g. Gapminder's [open-numbers](https://github.com/open-numbers/).

The most recent commit hash of the given branch will be used to determine whether the data has been updated. This way, the ETL will be triggered to rebuild any downstream steps each time the data is changed.

!!! example
    ```yaml title="dag/open_numbers.yml"
    data://open_numbers/open_numbers/latest/bp__energy:
    - github://open-numbers/ddf--bp--energy
    ```

!!! note
    Github rate-limits unauthorized API requests to 60 per hour, so we should be sparing with the use of this step as it is implemented today.

