"""Regression tests for boolean normalization of collector option fields."""
import unittest
from collectors.scripts.market_store import _bool


class MarketStoreBoolTests(unittest.TestCase):
    def test_boolean_inputs(self):
        self.assertIs(_bool(True), True)
        self.assertIs(_bool(False), False)

    def test_string_true_values(self):
        for value in ("true", "TRUE", "1", "yes", "Y", "on"):
            with self.subTest(value=value):
                self.assertIs(_bool(value), True)

    def test_string_false_values(self):
        for value in ("false", "FALSE", "0", "no", "N", "off", ""):
            with self.subTest(value=value):
                self.assertIs(_bool(value), False)

    def test_numeric_values(self):
        self.assertIs(_bool(1), True)
        self.assertIs(_bool(0), False)

    def test_unknown_and_null_values(self):
        self.assertIsNone(_bool(None))
        self.assertIsNone(_bool("unknown"))


if __name__ == "__main__":
    unittest.main()
