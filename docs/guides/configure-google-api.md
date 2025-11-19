---
icon: simple/google
tags:
  - 👷 Staff
---
# Google API configuration

In order to use the Google API for ETL tasks, you need to configure your Google Cloud Project and set up OAuth credentials. Follow these steps:

1. Create a Google Cloud Project:

    - Go to Google Cloud Console: [https://console.cloud.google.com/](https://console.cloud.google.com/) (ensure you are logged in with your owid google account)
    - Click "Select a project (top menu)" > "New Project."
    - Name the project `owid-etl-surname`, replacing surname with your last name (e.g., owid-etl-rosado).
    - Click "Create"

2. Enable Required APIs:

    - Navigate to "APIs & Services" > "Library"
    - Enable the following APIs: Google Drive API, Google Docs API, Google Sheets API

3. Create OAuth Credentials:

    - Go to "APIs & Services" > "Credentials."
    - Click "Create Credentials" > "OAuth Client ID."
    - ⚠️ If prompted, configure the consent screen:
        - Select "External" and click "Create"
        - Fill in the required fields (e.g., app name, email) and click "Save and Continue" until the process is complete.
    - Choose "Desktop App" as the application type.
    - Click "Create" and then "Download JSON" to save the client_secret.json file.
    - Save the downloaded file in your home directory in a hidden folder and rename as: `.config/owid_etl_doc/client_secret.json`

4. Generate your own token pickle:

    - Run the following code from the python terminal (or interactive window): `from etl.google import GoogleDrive; GoogleDrive()`
    - A browser window should appear. Log in with your owid google account. This will create a token.pickle in the same folder as your `client_secret.json`
