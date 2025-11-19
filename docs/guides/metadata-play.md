---
tags:
  - Metadata
icon: lucide/info
---

# Metadata in data pages
To better understand how the different metadata fields are mapped to the different elements in a data page, we have created a very simple app.

Simply execute

```shell
etlwiz metaplay
```

!!! note "Use the correct environment variables"

    Make sure to run this with the appropriate environment variables set (you need access to the database). This works best with your staging environment (accessible via Tailscale).

    You can define custom environment variables in the file `.env.staging` and then run:

    ```shell
    ENV_FILE=.env.staging etlwiz metaplay
    ```

