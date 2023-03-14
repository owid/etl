## Create Google Sheets

1. Copy [Fast-track template](https://docs.google.com/spreadsheets/d/1j_mclAffQ2_jpbVEmI3VOiWRBeclBAIr-U7NpGAdV9A/edit?usp=sharing), ideally to a shared [OWID Fast-track](https://drive.google.com/drive/folders/0AAjVjD6_217sUk9PVA) folder. Then in the sheet, copy your data into `raw_data` or `data` sheet. (**Don't create a new document!** You have to make a copy of the template, then you can remove sheets that you don't need.)

2. In sheet `dataset_meta`, fill in **title**, **short_name** and **version** of your dataset. If you're still experimenting, it's good practice to add **DRAFT** prefix to the title. You can remove the prefix it once your dataset is ready (just make sure not to change `short_name`, otherwise new dataset would be created)

3. You can **optionally** fill metadata of your source or variables, but it's gonna work even if you don't touch variables metadata and use non-underscored titles. Simply copy your data into `data` sheet, set **title**, **short_name**, **version** and you're good to go.

4. Click on `File -> Share -> Publish to Web` and share the entire document as csv and copy the link.

    _Do not "restrict access to your organization" even if your data is private. It is very unlikely anyone would access the link by chance._


Examples:

* [AI Index](https://docs.google.com/spreadsheets/d/199kcalCjxEyynzS9rdws87T91f18E4O5zjmprHYExnE/edit?usp=sharing)
* [Nuclear Warheads](https://docs.google.com/spreadsheets/d/1ReTohcxpo-dRvnXFzYG4N0YT-HoXh3b6AbG-qIaGTLI/edit?usp=sharing)
* [Gravitational waves](https://docs.google.com/spreadsheets/d/1NKoZMe6bkXMS29mORNw3o25cENx3MnpYFJk2y3cWXOc/edit?usp=sharing)


## Import it with Fast-track

1. Paste the link into `New Google Sheets URL` field in the form below. Make sure to check `Make dataset private`, this will let you explore your dataset in Admin without publishing it.

2. Submit the form and resolve all validation errors. Next time you successfully import it, it'll be available in `Existing Google Sheets` dropdown.

3. Explore your data in Admin, create charts, etc.

4. Fill in metadata in Google Sheet. It is recommended to **use `snake_case` format for variable names** in `data` sheet and set their titles in `variables_meta` sheet. Overwriting title then updates the variable instead of creating a new one.

5. Once you're happy with the dataset, rerun this form with `Make dataset private` unchecked.


## Not implemented

Features that are not implemented yet, but could be! Get in touch @mojmir

- [ ] Updating dataset previously imported via Admin
- [ ] Changing metadata via Admin
- [ ] Automatic Google Sheets generation from existing datasets
- [ ] Interactive country harmonisation
