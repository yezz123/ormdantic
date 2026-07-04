"""Naming helpers backed by the Rust extension."""

import re
from typing import Union

from ormdantic._native import import_native_extension

_ormdantic = import_native_extension(
    context="identifier naming",
    required_symbols=("snake_case",),
)


def snake_case(string: str) -> str:
    """Return a version of the string in `snake_case` format."""
    return str(_ormdantic.snake_case(string))


def get_words(string: str) -> list[str]:
    """Get a list of words in a string in the order they appear."""
    words = [it for it in re.split(r"\b|_", string) if re.match(r"[\d\w]", it)]
    words = _split_words_on_regex(words, re.compile(r"(?<=[a-z])(?=[A-Z])"))
    words = _split_words_on_regex(words, re.compile(r"(?<=[A-Z])(?=[A-Z][a-z])"))
    words = _split_words_on_regex(words, re.compile(r"(?<=\d)(?=[A-Za-z])"))
    return words


def _split_words_on_regex(
    words: list[str],
    regex: Union[re.Pattern, str],  # type: ignore
) -> list[str]:
    """Split a list of words on a regex, returning the split words."""
    words = words.copy()
    for i, word in enumerate(words):
        split_words = re.split(regex, word)
        if len(split_words) > 1:
            words.pop(i)
            for j, split_word in enumerate(split_words):
                words.insert(i + j, split_word)
    return words
