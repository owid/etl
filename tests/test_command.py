#
#  test_command.py
#

import time

from etl import command


def test_timed_run():
    time_taken = command.timed_run(lambda: time.sleep(0.05))
    assert abs(time_taken - 0.05) < 0.01
