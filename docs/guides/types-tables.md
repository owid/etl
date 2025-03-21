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

This process automatically converts all columns to the recommended types:

* `float64` or `float32` -> `Float64`
* `int32`, `uint32`, or `int64` -> `Int64`
* `category` -> `string[pyarrow]`

To disable this conversion, use `.read("my_table", safe_types=False)`. This is especially useful when working with large tables where conversion to string type would significantly increase memory usage.


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


!!! info "Difference between `string` and `string[pyarrow]`"

    Both types are very similar, but `string[pyarrow]` is more efficient and will be the default in Pandas 3.0. In practice, you won't notice a difference.


### `NaN` values
Avoid using `np.nan`! Always use `pd.NA`.
