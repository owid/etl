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

## Sharing private work with external people

If you created a private staging server with a `-private` suffix, the domain `https://<branch>.owid.pages.dev/` will not be publicly accessible. However, you can share it with specific external people by making it public initially and then protecting it with Cloudflare Access.

1. **Do not** use the `-private` suffix in your branch name.
2. Go to [Cloudflare Access -> Applications](https://one.dash.cloudflare.com/078fcdfed9955087315dd86792e71a7e/access/apps?search=) and edit the application [Cloudflare Pages (owid)](https://one.dash.cloudflare.com/078fcdfed9955087315dd86792e71a7e/access/apps/edit/d8c658c3-fd20-477e-ac20-e7ed7fd656de?tab=overview).
3. In the `Overview` tab, click on `+Add domain` and enter the subdomain of your staging server.
4. Go to the [Policies](https://one.dash.cloudflare.com/078fcdfed9955087315dd86792e71a7e/access/apps/edit/d8c658c3-fd20-477e-ac20-e7ed7fd656de?tab=policies) tab and edit the [External e-mails](https://one.dash.cloudflare.com/078fcdfed9955087315dd86792e71a7e/access/apps/rules/d8c658c3-fd20-477e-ac20-e7ed7fd656de/4c7bfba1-7bca-4e7c-8a32-5a11ab5f36fe) policy.
5. Add the e-mail addresses of the people you want to share the staging server with.

Once set up, the URL `https://<branch>.owid.pages.dev/` will require authentication. People with the e-mails you added in the policy will be able to access it via Google or by entering their e-mail address and using the code sent to them.
