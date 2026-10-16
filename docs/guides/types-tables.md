---
icon: material/table
---

# Types in Tables

In ETL, we work with Table object, which is derived from pandas.DataFrame to adjust it to our needs. Setting the types of your columns is crucial for performance and memory optimization. In this guide, we'll cover the types we use in our ETL pipeline and how to set them.

As a general summary, we use nullable numeric types (`Float64`, `Int64`) and pandas' default string type, `str`.

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

This process automatically converts all columns to the recommended types:

* `float64` or `float32` -> `Float64`
* `int32`, `uint32`, or `int64` -> `Int64`
* `category`, `object` or `string` -> `str`

To disable this conversion, use `.read("my_table", safe_types=False)`. This is especially useful when working with large tables where conversion to string type would significantly increase memory usage.


### Repacking datasets
We use a "repacking process" to reduce the size of the dataset before saving it to disk (`dataset.save()`). This process also converts the data to compact types (e.g. `category` for strings, the smallest integer type that fits), whatever types you used in the step.

### String dtypes
Use pandas' default string type, `str`. It is backed by pyarrow and marks missing values with `NaN`.

=== "✅ New"
    ```python
    tb["my_column"] = tb["my_column"].astype("str")
    ```

=== "❌ Old"
    ```python
    tb["my_column"] = tb["my_column"].astype("string")
    # or
    tb["my_column"] = tb["my_column"].astype("string[pyarrow]")
    ```

`string` and `string[pyarrow]` mark missing values with `pd.NA` instead, so mixing them with `str` causes surprises: comparisons return `<NA>` rather than `False`, and merging a `str` key with a `string` key turns it into `object`.

However, if you don’t set a string type, repack will handle the conversion when saving.


### Missing values
Numeric columns (`Float64`, `Int64`) mark missing values with `pd.NA`, and string columns (`str`) with `NaN`. Check for missing values with `pd.isna()` / `.isna()`, which handle both, never with `is pd.NA` or `== np.nan`.
