## Importing large CSV

If your dataset is too big to fit into Google Sheets (your CSV has >20mb), you can instead upload it somewhere else and tell fasttrack to import it from there.

1. Follow `Instructions for importing Google Sheet` to create a new dataset, but don't paste any data there and delete `data` and `raw_data` sheets.

2. Upload your CSV to a public URL. This could be Google Drive to [OWID Fast-track -> datasets](https://drive.google.com/drive/folders/1OeK3wsNnaHFCOTQlxHjqgZuFS9gvPhaI?usp=share_link) folder or it could be S3.

3. Instead of having data in Google Sheets, you will have a link to your CSV in `dataset_meta` sheet in `external_csv` field.

4. Get the URL of your CSV. For example, if you uploaded it to Google Drive, you can get the URL by right-clicking on the file and selecting `Get shareable link`. The URL should look something like this: `https://drive.google.com/file/d/1--xdZuBFD1ZgM_8e4frydCheab6KBgnU/view?usp=sharing`. Paste this URL into that `data_url` field.

5. Paste link of your Google Sheets just like you'd do in `Instructions for importing Google Sheet`
