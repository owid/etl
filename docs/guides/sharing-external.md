---
tags:
    - 👷 Staff
---
## Sharing work with external people

Sometimes it's useful to share our work with external people to get feedback before publishing it to the public. All staging servers are by default available to the public on `https://<branch>.owid.pages.dev/`.

<!-- Staging servers can be made available to public by creating a branch with `-public` suffix. This will make the staging site available at **https://staging-site-my-branch.tail6e23.ts.net**.

If you work on `my-branch` and create a `my-branch-public` branch, you'll have to sync your charts there with
```bash
etl chart-sync my-branch my-branch-public
```
If your charts don't appear on `https://staging-site-my-branch-public.tail6e23.ts.net/grapher/xyz`, try triggering manual deploy. -->


### Sharing explorers

To share explorers with the public, follow these steps:

1. Set `isPublished` to `true` in your explorer configuration.
2. Commit to trigger a deploy (could be empty commit with `--allow-empty`)
3. Share your explorer with public on e.g. https://<branch>.owid.pages.dev/explorers/my-explorer.


<!-- OLD INSTRUCTIONS
1. Create a branch with `-public` suffix (thus creating staging server).
2. Set `isPublished` to `true` in your explorer configuration.
3. Trigger manual deploy from Admin (this is only needed to do once) or commit to trigger it automatically.
4. Share your explorer with public on e.g. https://staging-site-my-branch.tail6e23.ts.net/explorers/my-explorer. -->
