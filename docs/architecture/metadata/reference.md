## origins

### producer

!!! info "`string` | mandatory"
    === "Description"
        Name of the institution or the author(s) that produced the dataset.

    === "Guidelines"

        - Must start with a capital letter. Exceptions:
            - The name of the institution or the author must be spelled with small letter, e.g. `van Haasteren`.
        - Must **not** end with a period. Exceptions:
            - When using `et al.` (for papers with multiple authors).
        - Must **not** include a date or year.
        - Must **not** mention `Our World in Data` or `OWID`.
        - Must **not** include any semicolon `;`.
        - Regarding authors:
            - One author: `Williams`.
            - Two authors: `Williams and Jones`.
            - Three or more authors: `Williams et al.`.
        - Regarding acronyms:
            - If the acronym is more well known than the full name, use just the acronym, e.g. `NASA`.
            - If the acronym is not well known, use the full name, e.g. `Energy Institute`.
            - If the institution explicitly asks, follow their guidelines, e.g. `Food and Agriculture Organization of the United Nations` (instead of `FAO`).


    === "Examples"

        | ✅ DOs      | ❌ DON'Ts                          |
        | ----------- | ------------------------------------ |
        | `NASA`       | `NASA (2023)`, `N.A.S.A.`, `N A S A`, `National Aeronautics and Space Administration`, `Our World in Data based on NASA` |
        | `Energy Institute`       | `EI` (not a well-known acronym). |
        | `Williams et al.`    | `Williams et al. (2023)`, `Williams et al`, `John Williams et al.` |
        | `van Haasteren et al.`    | `Van Haasteren et al.` |
        | `Williams and Jones`    | `Williams & Jones`, `John Williams and Indiana Jones` |



    === "When is it used?"



### `date_publisher`

