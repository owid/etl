---
icon: simple/google
tags:
  - 👷 Staff
---
# Google API configuration

ETL's Google helpers (`etl/google.py`: Docs, Drive and Sheets) sign in as you through one OAuth client that the whole team shares. It lives in the `owid-etl` Google Cloud project, and only `@ourworldindata.org` accounts can sign in through it. You don't need a Cloud project of your own.

1. Download [the shared `client_secret.json`](https://ourworldindata.1password.com/app#/E46VV72PBZFZXCCJCLRXIFV4WY/Vault/E46VV72PBZFZXCCJCLRXIFV4WY:tzic5fifbreubh4uzchmxk4tt4:mv2wimfc6lgiehhghd7u5lpmgm) from 1Password and save it as `~/.config/owid_etl_doc/client_secret.json`. Keep it out of git repositories: `owid/etl` is public.
2. Log in once. Run `from etl.google import GoogleDrive; GoogleDrive()` from a Python terminal or interactive window, and sign in with your OWID account in the browser window that opens. This saves `token.pickle` next to `client_secret.json`, so later runs don't ask again.

**If you set up a personal `owid-etl-<surname>` project before:** replace your `client_secret.json` with the shared one. The next run notices that your saved login belongs to the old client and asks you to sign in once more. After that, the old project can be deleted.
