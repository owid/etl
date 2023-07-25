from pytest import raises

from owid.datautils.format.numbers import (
    IntegerNumber,
    IntegerNumberWithSeparators,
    IntegerNumberWithWords,
    format_number,
    num_to_str,
    remove_multiple_whitespaces,
)


class TestIntegerNumberWithSeparators:
    separators = [",", ".", " "]
    numbers_valid = ["1,000,000", "1,234", "123,032", "21,000,100,231"]
    numbers_valid_corrected = [1000000, 1234, 123032, 21000100231]
    numbers_wrong = ["1,", "1,0", "2,01", "1234,000"]

    def test_init(self):
        number = IntegerNumberWithSeparators("2,000")
        assert number.number_raw == "2,000"
        assert isinstance(number.number_raw, str)

    def test_is_valid_ok(self):
        for num in self.numbers_valid:
            for sep in self.separators:
                assert IntegerNumberWithSeparators.is_valid(num.replace(",", sep))

    def test_is_valid_wrong(self):
        for num in self.numbers_wrong:
            for sep in self.separators:
                assert not IntegerNumberWithSeparators.is_valid(num.replace(",", sep))
        # Others
        assert not IntegerNumberWithSeparators.is_valid("1.000,213")
        assert not IntegerNumberWithSeparators.is_valid("1.000 213")
        assert not IntegerNumberWithSeparators.is_valid("1")
        assert not IntegerNumberWithSeparators.is_valid("hola")

        with raises(TypeError):
            assert not IntegerNumberWithSeparators.is_valid(1)  # type: ignore
        with raises(TypeError):
            assert not IntegerNumberWithSeparators.is_valid(1.000)  # type: ignore

    def test_clean_ok(self):
        for num, num_cor in zip(self.numbers_valid, self.numbers_valid_corrected):
            number = IntegerNumberWithSeparators(num).clean()
            assert number == num_cor

    def test_clean_wrong(self):
        for num in self.numbers_wrong:
            with raises(ValueError):
                _ = IntegerNumberWithSeparators(num).clean()


class TestIntegerNumberWithWords:
    def test_init(self):
        number = IntegerNumberWithWords("2 million")
        assert number.number_raw == "2 million"
        assert isinstance(number.number_raw, str)

    def test_regex_number_verbose(self):
        regex = IntegerNumberWithWords.regex_number_verbose()
        assert isinstance(regex, str)

    def test_numeric_words_list(self):
        words = IntegerNumberWithWords.numeric_words_list()
        assert isinstance(words, set)
        assert all([isinstance(word, str) for word in words])

    def test_match_numeric_words_ok(self):
        numbers = {
            "2 million": {
                "million": "2",
                "ten_thousand": 0,
                "thousand": 0,
                "hundred": 0,
                "one": 0,
            },
            "2 million and 1": {
                "million": "2",
                "ten_thousand": 0,
                "thousand": 0,
                "hundred": 0,
                "one": "1",
            },
            "2 million 1 thousand": {
                "million": "2",
                "ten_thousand": 0,
                "thousand": "1",
                "hundred": 0,
                "one": 0,
            },
            "2 million 2 hundred 1": {
                "million": "2",
                "ten_thousand": 0,
                "thousand": 0,
                "hundred": "2",
                "one": "1",
            },
            "2 million 3 thousand 2 hundred 1": {
                "million": "2",
                "ten_thousand": 0,
                "thousand": "3",
                "hundred": "2",
                "one": "1",
            },
        }
        for number_str, number_dix in numbers.items():
            number = IntegerNumberWithWords(number_str)
            dix = number._match_numeric_words()
            assert isinstance(dix, dict)
            assert dix == number_dix

    def test_match_numeric_words_wrong(self):
        numbers = ["hello", "trillion", "hundred"]
        for number_str in numbers:
            number = IntegerNumberWithWords(number_str)
            dix = number._match_numeric_words()
            assert isinstance(dix, dict)
            assert dix == {
                "million": 0,
                "ten_thousand": 0,
                "thousand": 0,
                "hundred": 0,
                "one": 0,
            }

    def test_build_IntegerNumber(self):
        number_equivalences = {
            0: {
                "million": 0,
                "ten_thousand": 0,
                "thousand": 0,
                "hundred": 0,
                "one": 0,
            },
            1: {
                "million": 0,
                "ten_thousand": 0,
                "thousand": 0,
                "hundred": 0,
                "one": 1,
            },
            101: {
                "million": 0,
                "ten_thousand": 0,
                "thousand": 0,
                "hundred": 1,
                "one": 1,
            },
            1203: {
                "million": 0,
                "ten_thousand": 0,
                "thousand": 1,
                "hundred": 2,
                "one": 3,
            },
            1007605: {
                "million": 1,
                "ten_thousand": 0,
                "thousand": 7,
                "hundred": 6,
                "one": 5,
            },
        }
        for number, number_dix in number_equivalences.items():
            number_ = IntegerNumberWithWords._build_IntegerNumber(number_dix)  # type: ignore
            assert number == number_


class TestNumber:
    def test_init(self):
        number = IntegerNumber(1)
        assert number.number_raw == 1
        assert number.number == "1"

    def test_init_clean(self):
        assert "1 000" == IntegerNumber.init_clean("1  000")

    def test_clean_correct(self):
        numbers = {
            1: 1,
            "1 000 000": 1000000,
            "1 000 100": 1000100,
            "2.000": 2000,
            "2 000": 2000,
            "2,000": 2000,
            "2 million": 2000000,
            "2  million": 2000000,
            "2  million 100 thousand 3 hundred 3": 2100303,
            "2  cientos": 200,
        }
        for number_raw, number_corrected in numbers.items():
            number = IntegerNumber(number_raw)  # type: ignore
            assert number_corrected == number.clean()

    def test_clean_wrong(self):
        numbers = [
            "2,0",
            "32,",
            "1.000,0",
            "1,000 thousand",
            "1 a",
        ]
        for num in numbers:
            number = IntegerNumber(num)
            with raises(ValueError):
                number.clean()


def test_num_to_str():
    assert "1" == num_to_str(1)
    assert "1" == num_to_str("1")


def test_remove_multiple_whitespaces():
    assert "1 million" == remove_multiple_whitespaces("1  million")


def test_format_IntegerNumber():
    assert 100 == format_number("100")
