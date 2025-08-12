## Dataset Update

1. First create a draft PR using `etl pr` command with a proper branch name and `data` category
2. Use the `etl update snapshot://` command with `--include-usages` to copy the dataset to the new version. For instance
   ```bash
   etl update snapshot://#$ARGUMENTS --include-usages
   ```
3. Run snapshot with `etls`, e.g. `etls #$ARGUMENTS`
4. Run `etlr` to execute the updated steps. It's ok if it fails!
5. Commit changes and push to the PR branch
