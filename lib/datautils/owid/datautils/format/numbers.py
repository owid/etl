"""Numeric formatting."""
import re
from typing import Any, Dict, Set, Union


class IntegerNumber:
    """Wrapper around integer numbers."""

    def __init__(self, number: Union[str, int]) -> None:
        self.number_raw = number
        self.number = self.init_clean(number)

    @classmethod
    def init_clean(cls, number: Union[int, str]) -> str:
        """First number cleaning steps.

        Parameters
        ----------
        number : Union[int, str]
            Raw number as given.

        Returns
        -------
        int
            Number as a string, wihtout repeated whitespaces.
        """
        # Force string
        number = num_to_str(number)
        # Remove multiple spaces
        number = remove_multiple_whitespaces(number)
        return number

    def clean(self) -> int:
        """Clean number."""
        if self.number.isnumeric():  # already a number
            return int(self.number)
        elif IntegerNumberWithWords.is_valid(self.number):  # contains words
            return IntegerNumberWithWords(self.number).clean()
        elif IntegerNumberWithSeparators.is_valid(self.number):  # has zero-separator
            return IntegerNumberWithSeparators(self.number).clean()
        else:  # error
            raise ValueError(f"Invalid number format {self.number}")


class IntegerNumberWithSeparators:
    """Class for numbers with separators.

    Accepted separators are: '.', ',' and ' '.
    """

    accepted_separators = [r"\.", ",", r"\s"]

    def __init__(self, number_raw: str) -> None:
        self.number_raw = number_raw

    @classmethod
    def regex_number_with_separator(cls) -> str:
        """Regex expression for number with separators."""
        accepted_separators_str = "|".join(rf"(({sep}\d{{3}})+)" for sep in cls.accepted_separators)
        regex_number_with_separator: str = rf"\d{{1,3}}({accepted_separators_str})"
        return regex_number_with_separator

    @classmethod
    def is_valid(cls, number: str) -> bool:
        """Check if given number has valid separators.

        Parameters
        ----------
        number : str
            Raw number.

        Returns
        -------
        bool:
            True if valid syntax.
        """
        return bool(re.fullmatch(cls.regex_number_with_separator(), number))

    def clean(self) -> int:
        """Clean number.

        Returns
        -------
        int
            Cleaned number (i.e. without separators).

        Raises
        ------
        ValueError:
            If provided number was not correct (e.g. does not contain a separator).
        """
        if self.is_valid(self.number_raw):
            n = re.sub(r"[^0-9]", "", str(self.number_raw))
            return int(n)
        raise ValueError(f"Given number {self.number_raw} is not valid!")


class IntegerNumberWithWords:
    """Class for numbers with words.

    Words suported can be found in class attribute `numeric_words`.

    Examples of accepted formats:

    - 1 million ten thousand
    - 20 thousand 1 hondred

    Examples of not supported formats:

    - 1 hundred thousand
    - one hundred
    - 1,22 thousand

    Use with caution!
    """

    numeric_words: Dict[str, Dict[str, Any]] = {
        "million": {
            "words": [
                "million",
                "millió",
                "millón",
                "millones",
                "millions",
                "millionen",
                "milioni",
                "milione",
                "miljoen",
                "milhão",
                "milhões",
            ],
            "factor": 1e6,
        },
        "ten_thousand": {
            "words": ["万"],
            "factor": 1e4,
        },
        "thousand": {
            "words": [
                "thousand",
                "ezren",
                "mil",
                "duizend",
                "mila",
                "mille",
                "tausend",
            ],
            "factor": 1e3,
        },
        "hundred": {
            "words": [
                "hundred",
                "cien",
                "cientos",
                "cent",
                "hundert",
                "honderd",
                "cem",
                "cento",
            ],
            "factor": 1e2,
        },
        "one": {"words": [""], "factor": 1},
    }
    regex_number_verbose_template: str = r"(?:(?P<{}>[1-9]\d{{,2}})\s?(?:{}))?"

    def __init__(self, number_raw: str) -> None:
        self.number_raw = number_raw
        self.number = self.init_clean(number_raw)

    @classmethod
    def init_clean(cls, number_raw: str) -> str:
        """Clean raw number."""
        return number_raw.replace(" and ", " ")

    @classmethod
    def regex_number_verbose(cls) -> str:
        """Build regex for a number with words.

        Returns
        -------
        str
            Regex.
        """
        regex = [
            cls.regex_number_verbose_template.format(k, "|".join(v["words"])) for k, v in cls.numeric_words.items()
        ]
        return r"\s?".join(regex)

    @classmethod
    def numeric_words_list(cls) -> Set[str]:
        """Return list of all numeric words (flattened)."""
        words = set(word for value in cls.numeric_words.values() for word in value["words"] if word != "")
        return words

    def _match_numeric_words(self) -> Dict[str, Union[str, int]]:
        """Match number with words."""
        match = re.search(self.regex_number_verbose(), self.number)
        if match:
            numbers = match.groupdict(default=0)
            return numbers
        else:
            raise ValueError("Number may not contain numeric words. Please review!")

    @classmethod
    def _build_IntegerNumber(cls, numbers: Dict[str, Union[str, int]]) -> int:
        """Build number from dictionary."""
        value = 0
        for k, v in numbers.items():
            value += float(v) * cls.numeric_words[k]["factor"]
        return int(value)

    @classmethod
    def is_valid(cls, number: str) -> bool:
        """Check if number contains numeric words.

        Parameters
        ----------
        number : str
            Candidate number.

        Returns
        -------
        bool
            True if number contains numeric valid words.
        """
        # return any(word in number for word in cls.numeric_words_list())
        return bool(re.fullmatch(cls.regex_number_verbose(), number))

    def clean(self) -> int:
        """Clean number.

        Returns:
        --------
        int:
            Cleaned number.
        """
        if self.is_valid(self.number_raw):
            number_dix = self._match_numeric_words()
            number = self._build_IntegerNumber(number_dix)
            return number
        raise ValueError(f"Given number {self.number_raw} is not valid!")


def num_to_str(num_as_str: Union[int, str]) -> str:
    """Convert number to string.

    Parameters
    ----------
    num_as_str: Union[int, str]
        Raw number.

    Returns:
    --------
    str:
        Raw number as string.
    """
    if not isinstance(num_as_str, str):
        return str(num_as_str)
    return num_as_str


def remove_multiple_whitespaces(text: str) -> str:
    """Remove excess of whitespaces.

    '  ' -> ' '.

    Parameters
    ----------
    text : str
        Raw text.

    Returns
    -------
    str
        String without duplicated whitespaces.
    """
    return re.sub(r"\s+", " ", text)


def format_number(number: Union[int, str]) -> int:
    """Format number.

    Only supports integer conversion.

    Some examples of transformations are:

        - '1 000 000' -> 1000000
        - '1 million 1 hundred' -> 1000100
        - '2,000' -> 2000

    Parameters
    ----------
    number : Union[int, str]
        Input raw number to be formatted.

    Returns
    -------
    int
        Formatted number.
    """
    number_ = IntegerNumber(number)
    return number_.clean()
