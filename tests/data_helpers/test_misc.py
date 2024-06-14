"""Test functions in etl.data_helpers.misc module.

"""

from etl.data_helpers.misc import round_to_nearest_power_of_ten, round_to_sig_figs


def test_round_to_sig_figs_1_sig_fig():
    tests = {
        0.01: 0.01,
        0.059: 0.06,
        0.055: 0.06,
        0.050: 0.05,
        0.0441: 0.04,
        0: 0,
        1: 1,
        5: 5,
        9: 9,
        10: 10,
        11: 10,
        15: 20,
        440.0321: 400,
        450.0321: 500,
        987: 1000,
    }
    for test in tests.items():
        assert round_to_sig_figs(test[0], sig_figs=1) == float(test[1])
        # Check also the same numbers but negative.
        assert round_to_sig_figs(-test[0], sig_figs=1) == -float(test[1])


def test_round_to_sig_figs_2_sig_fig():
    tests = {
        0.01: 0.010,
        0.059: 0.059,
        0.055: 0.055,
        0.050: 0.050,
        0.0441: 0.044,
        0: 0.0,
        1: 1.0,
        5: 5.0,
        9: 9.0,
        10: 10,
        11: 11,
        15: 15,
        440.0321: 440,
        450.0321: 450,
        987: 990,
    }
    for test in tests.items():
        assert round_to_sig_figs(test[0], sig_figs=2) == test[1]
        # Check also the same numbers but negative.
        assert round_to_sig_figs(-test[0], sig_figs=2) == -test[1]


def test_round_to_nearest_power_of_ten_floor():
    tests = {
        -0.1: -0.1,
        -0.12: -0.1,
        -90: -10,
        0: 0,
        1: 1,
        123: 100,
        1001: 1000,
        9000: 1000,
        0.87: 0.1,
        0.032: 0.01,
        0.0005: 0.0001,
    }
    for test in tests.items():
        assert round_to_nearest_power_of_ten(test[0]) == test[1], test


def test_round_to_nearest_power_of_ten_ceil():
    tests = {
        -0.1: -0.1,
        -0.12: -1,
        -90: -100,
        0: 0,
        1: 1,
        123: 1000,
        1001: 10000,
        9000: 10000,
        0.87: 1,
        0.032: 0.1,
        0.0005: 0.001,
    }
    for test in tests.items():
        assert round_to_nearest_power_of_ten(test[0], floor=False) == test[1], test
