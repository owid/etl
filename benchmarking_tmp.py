import pandas as pd
import os
import gzip
import shutil
import timeit

from tempfile import TemporaryDirectory as TempDir

ITERATIONS = 100
TEST_DF = pd.read_feather(
    'https://owid-catalog.nyc3.digitaloceanspaces.com/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp.feather')


def read_feather_gz(feather_gz_file: str) -> pd.DataFrame:
    """Helper to read gzipped feather"""
    with gzip.open(feather_gz_file, 'rb') as raw_feather:
        return pd.read_feather(raw_feather)


def benchmark():
    """Compare the compressed size + read performance for zstd and gzip compression of feather files"""
    print('Dataframe len: {}'.format(len(TEST_DF.index)))

    with TempDir() as tempdir:
        # Write feather with native compression
        zstd_feather = os.path.join(tempdir, 'df_zstd.feather')
        TEST_DF.to_feather(zstd_feather, compression='zstd')

        # Write uncompressed feather, then gzip
        raw_feather = os.path.join(tempdir, 'df_raw.feather')
        raw_feather_gz = raw_feather + '.gz'
        TEST_DF.to_feather(raw_feather, compression='uncompressed')
        with open(raw_feather, 'rb') as f_in:
            with gzip.open(raw_feather_gz, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Print file sizes to compare
        print('Size of zstd feather: {:,} bytes'.format(os.path.getsize(zstd_feather)))
        print('Size of gzipped raw feather: {:,} bytes'.format(os.path.getsize(raw_feather_gz)))

        # Profile read time for each type
        zstd_timer = timeit.Timer(
            '_ = pd.read_feather(zstd_feather)',
            globals={'zstd_feather': zstd_feather},
            setup='import pandas as pd')
        zstd_time = zstd_timer.timeit(ITERATIONS) / ITERATIONS

        gz_timer = timeit.Timer(
            '_ = read_feather_gz(raw_feather_gz)',
            globals={'read_feather_gz': read_feather_gz, 'raw_feather_gz': raw_feather_gz})
        gz_time = gz_timer.timeit(ITERATIONS) / ITERATIONS

        print('Average read time (zstd): {:.5f}s'.format(zstd_time))
        print('Average read time (uncompressed + gz): {:.5f}s'.format(gz_time))


if __name__ == "__main__":
    benchmark()