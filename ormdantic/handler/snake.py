import re
from typing import Union


def snake(string: str) -> str:
    """Return a version of the string in `snake_case`` format."""
    return "_".join(map(lambda w: w.lower(), get_words(string)))


def get_words(string: str) -> list[str]:
    """Get a list of the words in a string in the order they appear."""
    words = [it for it in re.split(r"\b|_", string) if re.match(r"[\d\w]", it)]
    # Split on lower then upper: "oneTwo" -> ["one", "Two"]
    words = _split_words_on_regex(words, re.compile(r"(?<=[a-z])(?=[A-Z])"))
    # Split on upper then upper + lower: "JSONWord" -> ["JSON", "Word"]
    words = _split_words_on_regex(words, re.compile(r"(?<=[A-Z])(?=[A-Z][a-z])"))
    # Split on number + letter: "TO1Cat23dog" -> ["TO1", "Cat23", "dog"]
    words = _split_words_on_regex(words, re.compile(r"(?<=\d)(?=[A-Za-z])"))
    return words


def _split_words_on_regex(words: list[str], regex: Union[re.Pattern, str]) -> list[str]:  # type: ignore
    """Split a list of words on a regex, returning the split words."""
    words = words.copy()
    for i, word in enumerate(words):
        split_words = re.split(regex, word)
        if len(split_words) > 1:
            words.pop(i)
            for j, sw in enumerate(split_words):
                words.insert(i + j, sw)
    return words
