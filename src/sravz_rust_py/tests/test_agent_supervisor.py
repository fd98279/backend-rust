import additional_imports  # pylint: disable=C0411
import unittest
# Get the parent directory of the current file
import os
import sys
from sravz_rust_py.sravz_llm_agent_tools import llm
import sravz_rust_py.agent_supervisor as agent_supervisor  # pylint: disable=C0411
import logging
from langchain.callbacks import get_openai_callback


class TestAgentSupervisor(unittest.TestCase):
    def setUp(self):
        self.query_fn = agent_supervisor.query

    def test_ytd_direction(self):
        funds = ["fund_us_fbgrx.json",
                 "fund_us_fsptx.json", "fund_us_fgrcx.json"]
        keys = ["Yield_1Year_YTD", "Yield_3Year_YTD", "Yield_5Year_YTD"]
        query_str = f"""
    Check if yield has been deceasing or increasing over time and store in value Yield_Direction.
    Order the funds by yield direction.
    Output data with columns Code, ${",".join(keys)}, Yield_Direction.
    """
        output = self.query_fn(funds, keys, query_str)
        print(output)
        self.assertIsNotNone(output)

    def test_get_yield_expense_ratio(self):
        funds = ["fund_us_fbgrx.json",
                 "fund_us_fsptx.json", "fund_us_fgrcx.json"]
        keys = ["Yield", "Expense_Ratio"]
        query = """
        Order the funds by decreasing yield by expense ratio.
        Output data with columns Code, Yield, Expense_Ratio and Yield/Expense_Ration as YER-Ratio.
    """
        output = self.query_fn(funds, keys, query)
        print(output)
        self.assertIsNotNone(output)

    def test_get_top_holdings(self):
        funds = ["fund_us_fbgrx.json",
                 "fund_us_fsptx.json", "fund_us_fgrcx.json"]
        keys = ["Top_Holdings"]
        query = """
        For each asset get the 5 Top_Holdings based on Weight and capture data with code, asset and Weight columns.
    """
        output = self.query_fn(funds, keys, query)
        print(output)
        self.assertIsNotNone(output)

    def test_fund_summary(self):
        funds = ["fund_us_fbgrx.json", "fund_us_fsptx.json",
                 "fund_us_fgrcx.json",  "fund_us_fzalx.json", "fund_us_aaenx.json"]
        keys = ["Fund_Summary"]
        query = """
        Based on the Fund_Summary order the funds by most to least aggressive funds.
        Get data with columns Code and Aggressiveness.
        """
        output = self.query_fn(funds, keys, query)
        print(output)
        self.assertIsNotNone(output)

    def test_market_news_summary(self):
        funds = []
        keys = []
        query_header = "Please summarize the market news in a few bullet points."
        query = ""
        query_footer = """
        Save the summarized market news bullet points as a well formatted html to local file system and get full path of the file on the local file system.
        Upload the file on the local file system to Contabo and return the presigned URL."""
        output = self.query_fn(funds, keys, query,
                               query_header=query_header,
                               query_footer=query_footer)
        print(output)
        self.assertIsNotNone(output)

    def test_recommend_based_on_yield_direction(self):
        with get_openai_callback() as cost:
            funds = ["fund_us_fbgrx.json",
                     "fund_us_fsptx.json", "fund_us_fgrcx.json"]
            keys = ["Yield_1Year_YTD", "Yield_3Year_YTD", "Yield_5Year_YTD"]
            query_str = f"""
            Check if yield has been deceasing or increasing over time and store in value Yield_Direction.
            Order the funds by yield direction.
            Output data with columns Code, ${",".join(keys)}, Yield_Direction.
            """
            query_data, app = llm.stream_llm(
                funds, keys, query_str)

            config = {
                "configurable": {
                    "thread_id": "3"
                }
            }

            for chunk in app.stream(
                {
                    "messages": [
                    {
                        "role": "user",
                        "content": f"""
                            ${query_data.header}
                            ${query_data.query} 
                            ${query_data.footer}
                        """
                    }
                    ]
                },
                # highlight-next-line
                config,
                stream_mode="values"
            ):
                chunk["messages"][-1].pretty_print()

            for chunk in app.stream(
                {"messages": [
                    {"role": "user", "content": """
                    Based on the yield direction of the funds calculated so far, recommend which mutual fund is better.
                    Save the recommendation as a well formatted html to local file system and get full path of the file on the local file system.
                    Upload the file on the local file system to Contabo and return the presigned URL.
                    """}]},
                # highlight-next-line
                config,
                stream_mode="values"
            ):
                chunk["messages"][-1].pretty_print()

            # logger.info cost
            logging.info("total tokens: %s", cost.total_tokens)
            # Tokens in the question
            logging.info("prompt tokens: %s", cost.prompt_tokens)
            # Tokens used to generated response
            logging.info("completion tokens: %s", cost.completion_tokens)
            logging.info("cost is: %s", cost.total_cost)
