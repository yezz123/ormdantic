from unittest import TestCase

from ormdantic.handler.snake import get_words


class SnakeTest(TestCase):
    def __init__(self, *args) -> None:  # type: ignore
        self.snake_sample = "hello_yezz_data_happy"
        super().__init__(*args)

    def test_get_words_from_snake(self) -> None:
        self.assertEqual(
            ["hello", "yezz", "data", "happy"], get_words(self.snake_sample)
        )

    def test_get_words_from_snake_with_uppercase(self) -> None:
        self.assertEqual(
            ["HELLO", "YEZZ", "DATA", "HAPPY"], get_words(self.snake_sample.upper())
        )

    def test_get_words_from_snake_with_uppercase_and_underscore(self) -> None:
        self.assertEqual(
            ["HELLO", "YEZZ", "DATA", "HAPPY"],
            get_words(f"{self.snake_sample.upper()}_"),
        )

    def test_get_words_from_snake_with_underscore(self) -> None:
        self.assertEqual(
            ["hello", "yezz", "data", "happy"], get_words(f"{self.snake_sample}_")
        )
