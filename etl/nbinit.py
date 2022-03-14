# flake8: noqa
"""Collection of imports that are regularly used in `etl` notebooks. Run `from etl.nbinit import *` to
avoid listing them all in the notebook.
"""
import os
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import logging
import imp
import json
import sys
from IPython import get_ipython

ipython = get_ipython()
ipython.magic("load_ext rich")
ipython.magic("load_ext autoreload")
ipython.magic("autoreload 2")
ipython.magic("pylab inline")
ipython.magic("config InlineBackend.figure_format = 'svg'")


def set_logging_level(level: str) -> None:
    imp.reload(logging)
    logging.basicConfig(
        format="%(asctime)s %(levelname)s:%(message)s",
        level=getattr(logging, level),
        datefmt="%I:%M:%S",
    )
