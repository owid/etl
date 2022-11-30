1. Copy [Fast-track template](https://docs.google.com/spreadsheets/d/1j_mclAffQ2_jpbVEmI3VOiWRBeclBAIr-U7NpGAdV9A/edit?usp=sharing) and copy your data into `data` sheet. You can leave metadata blank and check the `Fill metadata` checkbox to automatically fill in the metadata from the data sheet.

2. Click on `File -> Share -> Publish to Web` and share the entire document as csv. Copy the link to the form below. Next time you successfully import it, it'll be available in `Existing Google Sheets` dropdown.

3. Make sure to check `Make dataset private`, this will let you explore your dataset in Admin without publishing it.

4. Submit the form and resolve all validation errors.

5. Explore your data in Admin, create charts, etc.

6. Fill in metadata in Google Sheet. It is recommended to **use `snake_case` format for variable names** in `data` sheet and set their titles in `variables_meta` sheet. Overwriting title then updates the variable instead of creating a new one.

7. Once you're happy with the dataset, rerun this form with `Make dataset private` unchecked.

Examples:

* [Nuclear Warheads](https://docs.google.com/spreadsheets/d/1ReTohcxpo-dRvnXFzYG4N0YT-HoXh3b6AbG-qIaGTLI/edit?usp=sharing)

## Not implemented

Features that are not implemented yet, but could be! Get in touch @mojmir

- [ ] Updating dataset previously imported via Admin
- [ ] Changing metadata via Admin
- [ ] Automatic Google Sheets generation from existing datasets
- [ ] Interactive country harmonisation
