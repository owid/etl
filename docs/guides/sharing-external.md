---
tags:
    - 👷 Staff
---
## Sharing work with external people

Sometimes it's useful to share our work with external people to get feedback before publishing it to the public. Staging servers can be made available to public by creating a branch with `-public` suffix. This will make the staging site available at https://staging-site-my-branch.tail6e23.ts.net.

### Sharing explorers

To share explorers with the public, follow these steps:

1. Create a branch wiht `-public` suffix (thus creating staging server).
2. Set `isPublished` to `true` in your explorer configuration.
3. Trigger manual deploy from Admin (this is only needed to do once) or commit to trigger it automatically.
4. Share your explorer with public on e.g. https://staging-site-my-branch.tail6e23.ts.net/explorers/my-explorer.
