---
status: new
---

In ETL, we work with Table object, which is derived from pandas.DataFrame to adjust it to our needs. Setting the types of your columns is crucial for performance and memory optimization. In this guide, we’ll cover the types we use in our ETL pipeline and how to set them.

As a general summary, we use nullable data types, and therefore recommend the usage of Float64, Int64 and string[pyarrow] types. We also avoid using np.nan and prefer pd.NA.

### Loading tables
The preferred way to load a table from a dataset is

=== "✅ New"
    ```python
    tb = ds_meadow.read("my_table")
    ```
=== "❌ Old"
    ```python
    tb = ds_meadow["my_table"]
    ```

This automatically converts all columns to the recommended types. (This method also includes extra parameters for memory optimization.)

### Repacking datasets
We use a "repacking process" to reduce the size of the dataset before saving it to disk (`dataset.save()`). This process also converts the data to the recommended types, even if you have converted the data to old NumOy types (e.g. `.astype(float)`).

### String dtypes
To convert a column to a string type use

=== "✅ New"
    ```python
    # Option 1
    tb["my_column"] = tb["my_column"].astype("string")
    # Option 2
    tb["my_column"] = tb["my_column"].astype("string[pyarrow]")
    ```

=== "❌ Old"
    ```python
    tb["my_column"] = tb["my_column"].astype(str)
    ```

However, if you don’t use the new method, repack will handle this conversion when saving.

### `NaN` values
Avoid using `np.nan`! Always use `pd.NA`.
