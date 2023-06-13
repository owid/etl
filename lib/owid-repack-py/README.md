# owid-repack-py

![build status](https://github.com/owid/owid-repack-py/actions/workflows/python-package.yml/badge.svg)
[![PyPI version](https://badge.fury.io/py/owid-repack.svg)](https://badge.fury.io/py/owid-repack)
![](https://img.shields.io/badge/python-3.8|3.9|3.10|3.11-blue.svg)

_Pack Pandas DataFrames into smaller, more memory efficient types._

## Overview

When you load data into Pandas, it will use standard types by default:

- `object` for strings
- `int64` for integers
- `float64` for floating point numbers

However, for many datasets there is a much more compact representation that Pandas could be using for that data. Using a more compact representation leads to lower memory usage, and smaller binary files on disk when using formats such as Feather and Parquet.

This library does just one thing: it shrinks your data frames to use smaller types.

## Installing

`pip install owid-repack`

## Usage

The `owid.repack` module exposes two methods, `repack_series()` and `repack_frame()`.

`repack_series()` will detect the smallest type that can accurately fit the existing data in the series.

```ipython
In [1]: from owid import repack

In [2]: pd.Series([1, 2, 3])
Out[2]:
0    1
1    2
2    3
dtype: int64

In [3]: repack.repack_series(pd.Series([1.5, 2, 3]))
Out[3]:
0    1.5
1    2.0
2    3.0
dtype: float32

In [4]: repack.repack_series(pd.Series([1, None, 3]))
Out[4]:
0       1
1    <NA>
2       3
dtype: UInt8

In [5]: repack.repack_series(pd.Series([-1, None, 3]))
Out[5]:
0      -1
1    <NA>
2       3
dtype: Int8
```

The `repack_frame()` method simply does this across every column in your DataFrame, returning a new DataFrame.

## Releases

- `0.1.2`:
    - Shrink columns with all NaNs to Int8
- `0.1.1`:
    - Fix Python support in package metadata to support 3.8.1 onwards
- `0.1.0`:
  - Migrate first version from `owid-catalog-py` repo
