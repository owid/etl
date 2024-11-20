In ETL, we work with Table object, which is derived from pandas.DataFrame to adjust it to our needs. Setting the types of your columns is crucial for performance and memory optimization. In this guide, we’ll cover the types we use in our ETL pipeline and how to set them.

As a general summary, we use nullable data types, and therefore recommend the usage of Float64, Int64 and string[pyarrow] types. We also avoid using np.nan and prefer pd.NA.

### Loading tables
The preferred way to load a table from a dataset is now tb = ds_meadow.read("my_table") instead of ds_meadow["my_table"], which automatically converts everything to the recommended types. (This method also includes extra parameters for memory optimization.)

### Repacking datasets
The repack process (which reduces dataset size before saving) now uses the new types. Even if you convert data to old NumPy types (e.g., .astype(float)), `repack` will convert them to the appropriate types on save. You’ll get the recommended types in the next step.


### String dtypes
Instead of .astype(str), it’s better to use .astype("string[pyarrow]") or .astype("string"). However, if you don’t, repack will handle this conversion when saving.

### NaN values
No more np.nan: Avoid using `np.nan`! Always use pd.NA —your sanity will thank you.


!!! note
Migration notes:
- This migration includes a Pandas update, so remember to run make .venv.
- If you prefer to download the catalog instead of rebuilding it, run:
  PREFER_DOWNLOAD=1 etlr garden --private --workers 4 :thread: (edited)
