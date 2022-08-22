import os

import pandas as pd  # noqa
from IPython import get_ipython
from sqlalchemy import create_engine

engine = create_engine(os.environ["MYSQL_URL"])

ipython = get_ipython()
ipython.magic("load_ext rich")
ipython.magic("load_ext autoreload")
ipython.magic("autoreload 2")
ipython.magic("pylab inline")
ipython.magic("config InlineBackend.figure_format = 'svg'")
