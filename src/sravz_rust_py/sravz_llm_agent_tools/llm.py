import os
import json

from functools import partial
from langchain_openai import ChatOpenAI
from langgraph_supervisor import create_supervisor
from langchain_helper import get_relevant_documents
from sravz_llm_agent_tools import tools, agents
from sravz_llm_agent_tools.QueryResponse import QueryResponse
from sravz_llm_agent_tools.QueryData import QueryData
from . import tools, llm_settings
from langgraph.checkpoint.redis import RedisSaver


def query_llm(funds, keys, query, query_header=None, query_footer=None) -> str:
    """
    Queries a language model with financial data and returns processed results.

    This function processes financial data through a workflow of specialized agents,
    including research, JSON processing, mathematical operations, and file operations.

    Args:
        llm_model: The language model to be used for processing
        funds(list): List of financial assets to be processed
        keys(list): List of keys to extract values from the financial data
        query(str): The specific query to be processed by the language model

    Returns:
        QueryResponse: A structured response containing the processed results

    Example:
        >> > query_llm(gpt4_model, ["AAPL", "GOOGL"], ["price", "volume"], "Calculate total market value")

    Notes:
        - The function creates a supervised workflow with multiple specialized agents
        - Processes JSON financial data
        - Saves results as HTML table locally
        - Uploads to Contabo storage
        - Returns a presigned URL for the uploaded file
    """
    query_footer = query_footer or """
                           Save the data as a well formatted html table to local file system and get full path of the file on the local file system.
                           Upload the file on the local file system to Contabo and return the presigned URL."""
    query_header = query_header or ""
    query_data = QueryData(keys=keys,
                           query=query,
                           asset_list=funds,
                           header=query_header,
                           footer=query_footer
                           )

    json_input_data_dict = {}

    if funds and keys:
        json_input_data_dict = get_relevant_documents(query_data)
        query_data.header = f"""
        Use this financial assets data in JSON format: {json.dumps(json_input_data_dict)}.
        From the financial data in JSON format, extract the values for keys ${",".join(keys)}."""
        agents.json_agent.tools = [partial(tools.get_value_a_key,
                                           json_input_data_dict=json_input_data_dict)]

    # Create supervisor workflow
    workflow = create_supervisor(
        [agents.research_agent, agents.json_agent,
            agents.math_agent,
            agents.save_to_file_agent,
            agents.save_to_contabo_and_get_presigned_url_agent,
            agents.market_news_agent],
        model=ChatOpenAI(model=llm_settings.LLM_MODEL),
        prompt=(
            "You are a team supervisor managing a research expert json expert save_to_file expert and a math expert. "
            "For current events, use research_agent. "
            "For math problems, use math_agent."
            "For json problems, use json_agent."
            "For file saving problems, use save_to_file_agent."
            "For save file to contabo and return presigned url, use save_to_contabo_and_get_presigned_url_agent."
            "For market news, use market_news_agent."
        ),
        response_format=QueryResponse
    )

    # Compile and run
    app = workflow.compile()
    app.get_graph().draw_mermaid_png(output_file_path="/tmp/workflow_graph.png")
    llm_call_result = app.invoke({
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
    })

    return str(llm_call_result['structured_response'])


def stream_llm(funds, keys, query, query_header=None, query_footer=None) -> str:
    """
    Queries a language model with financial data and returns processed results.

    This function processes financial data through a workflow of specialized agents,
    including research, JSON processing, mathematical operations, and file operations.

    Args:
        llm_model: The language model to be used for processing
        funds(list): List of financial assets to be processed
        keys(list): List of keys to extract values from the financial data
        query(str): The specific query to be processed by the language model

    Returns:
        QueryResponse: A structured response containing the processed results

    Example:
        >> > query_llm(gpt4_model, ["AAPL", "GOOGL"], ["price", "volume"], "Calculate total market value")

    Notes:
        - The function creates a supervised workflow with multiple specialized agents
        - Processes JSON financial data
        - Saves results as HTML table locally
        - Uploads to Contabo storage
        - Returns a presigned URL for the uploaded file
    """
    query_footer = query_footer or """
                           Save the data as a well formatted html table to local file system and get full path of the file on the local file system.
                           Upload the file on the local file system to Contabo and return the presigned URL."""
    query_header = query_header or ""
    query_data = QueryData(keys=keys,
                           query=query,
                           asset_list=funds,
                           header=query_header,
                           footer=query_footer
                           )

    json_input_data_dict = {}

    if funds and keys:
        json_input_data_dict = get_relevant_documents(query_data)
        query_data.header = f"""
        Use this financial assets data in JSON format: {json.dumps(json_input_data_dict)}.
        From the financial data in JSON format, extract the values for keys ${",".join(keys)}."""
        agents.json_agent.tools = [partial(tools.get_value_a_key,
                                           json_input_data_dict=json_input_data_dict)]

    # Set up Redis connection
    REDIS_URI = "redis://:100DaysAroundTheWorldR@redis:6379"
    memory = None
    with RedisSaver.from_conn_string(REDIS_URI) as cp:
        cp.setup()
        memory = cp

    all_agents = [agents.research_agent, agents.json_agent,
                  agents.math_agent,
                  agents.save_to_file_agent,
                  agents.save_to_contabo_and_get_presigned_url_agent,
                  agents.market_news_agent]

    for agent in all_agents:
        agent.memory = memory

    # Create supervisor workflow
    workflow = create_supervisor(
        all_agents,
        model=ChatOpenAI(model=llm_settings.LLM_MODEL),
        prompt=(
            "You are a team supervisor managing a research expert json expert save_to_file expert and a math expert. "
            "For current events, use research_agent. "
            "For math problems, use math_agent."
            "For json problems, use json_agent."
            "For file saving problems, use save_to_file_agent."
            "For save file to contabo and return presigned url, use save_to_contabo_and_get_presigned_url_agent."
            "For market news, use market_news_agent."
        ),
        response_format=QueryResponse
    )

    # Compile and run
    app = workflow.compile(checkpointer=memory)
    return query_data, app
