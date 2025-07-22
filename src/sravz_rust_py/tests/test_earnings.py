import unittest
import os
import additional_imports  # pylint: disable=C0411
import earnings  # pylint: disable=C0411


class TestEarnings(unittest.TestCase):
    def test_earnings(self):
        output_file_path = earnings.main('stk_us_nvda', os.path.join(
            additional_imports.current_file_dir, "earnings.parquet"), "/tmp/earnings.png")

        self.assertTrue(os.path.exists(output_file_path))
