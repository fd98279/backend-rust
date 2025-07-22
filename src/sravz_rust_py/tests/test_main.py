import unittest
import os
import additional_imports  # pylint: disable=C0411
import main
import PyMessage  # pylint: disable=C0411


class TestMain(unittest.TestCase):
    def test_message_id_1_leveraged_funds(self):
        py_message = PyMessage.PyMessage(
            "1",
            "",
            "fund_us_fbgrx.json,fund_us_fsptx.json,fund_us_fgrcx.json,fund_us_ekoax.json,fund_us_fzalx.json",
            "",
            "")
        main.run(py_message)
        self.assertTrue(os.path.exists(py_message.output))

    def test_message_id_2_query_llm(self):
        keys = ','.join(
            ["Yield_1Year_YTD", "Yield_3Year_YTD", "Yield_5Year_YTD"])
        py_message = PyMessage.PyMessage(
            "2",
            "",
            "fund_us_fbgrx.json,fund_us_fsptx.json,fund_us_fgrcx.json,fund_us_ekoax.json,fund_us_fzalx.json",
            "",
            "",
            keys,
            f"""
        Check if yield has been deceasing or increasing over time and store in value Yield_Direction.
        Order the funds by yield direction.
        Output data with columns Code, ${",".join(keys)}, Yield_Direction.
        """
        )
        main.run(py_message)
        print(py_message.output)
        self.assertIsNotNone(py_message.output)

    def test_message_id_3_earnings(self):
        py_message = PyMessage.PyMessage(
            "3",
            "",
            "stk_us_nvda",
            "",
            os.path.join(additional_imports.current_file_dir,
                         "earnings.parquet")
        )
        main.run(py_message)
        self.assertTrue(os.path.exists(py_message.output))
