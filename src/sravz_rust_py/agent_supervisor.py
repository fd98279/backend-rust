'''
    Run LLM query using langGraph
'''
from langchain.callbacks import get_openai_callback
from sravz_llm_agent_tools import llm
from utils import logger_setup

logger = logger_setup.get_logger(__name__)


def query(funds, keys, query_str, query_header=None, query_footer=None) -> str:
    """
    Query LLM with the provided funds, keys, and query while tracking token usage and cost.

    Args:
        funds (float): Available funds/budget for the LLM query
        keys (dict): Dictionary containing API keys and credentials 
        query (str): The query/prompt to send to the LLM

    Returns:
        None

    Raises:
        None explicitly, but may raise exceptions from underlying query_llm function

    Notes:
        Uses the OpenAI callback context manager to track:
        - Total tokens used
        - Prompt tokens used
        - Completion tokens used 
        - Total cost of the query

        Results are logged using the logger object.
    """
    query_response = ""
    

    with get_openai_callback() as cost:
        query_response = llm.query_llm(
            funds, keys, query_str, query_header=query_header, query_footer=query_footer)

        # logger.info cost
        logger.info("total tokens: %s", cost.total_tokens)
        # Tokens in the question
        logger.info("prompt tokens: %s", cost.prompt_tokens)
        # Tokens used to generated response
        logger.info("completion tokens: %s", cost.completion_tokens)
        logger.info("cost is: %s", cost.total_cost)

    return query_response


if __name__ == "__main__":
    pass
#     funds = ["fund_us_fbgrx.json", "fund_us_fsptx.json", "fund_us_fgrcx.json"]
#     keys = ["Yield_1Year_YTD", "Yield_3Year_YTD", "Yield_5Year_YTD"]
#     query_str = f"""
# Check if yield has been deceasing or increasing over time and store in value Yield_Direction.
# Order the funds by yield direction.
# Output data with columns Code, ${",".join(keys)}, Yield_Direction.
# """

#     query(funds, keys, query_str)
