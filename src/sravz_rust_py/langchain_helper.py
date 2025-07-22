#!/usr/bin/env python3
from __future__ import annotations
import settings
import sys

import json

import pandas as pd
# Python REPL
from langchain.agents import Tool
# Dataframe Agent
from langchain.callbacks import get_openai_callback
from langchain.globals import set_debug
# Tool
from langchain.tools import Tool
from langchain.tools.json.tool import JsonSpec
# Generic Agent
from langchain_community.agent_toolkits.json.base import create_json_agent
# LLM
from langchain_community.chat_models import ChatOpenAI
# Loaders
# JSON Agent
from langchain_community.embeddings import SentenceTransformerEmbeddings
# Other imports
from langchain_core.callbacks import StdOutCallbackHandler
# Embeddings
from langchain_core.documents import Document
from langchain_core.tools import Tool
from langchain_experimental.agents.agent_toolkits import \
    create_pandas_dataframe_agent
from langchain_experimental.tools.python.tool import PythonAstREPLTool
from langchain_experimental.utilities import PythonREPL
# Text splitters
from langchain_text_splitters import RecursiveJsonSplitter

from aws import AWSHelper
from json_toolkit_extended import JsonToolkitExtended
from JSONLoaderExtended import JSONLoaderExtended
from utils import logger_setup
from sravz_llm_agent_tools import QueryData

logger = logger_setup.get_logger(__name__)

# SQLLite
__import__('pysqlite3')

sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')


logger = logger_setup.get_logger(__name__)

KEYS_TO_EXCLUDE = ["Holdings"]

aws_helper = AWSHelper()
embedding_function = SentenceTransformerEmbeddings(
    model_name="all-MiniLM-L6-v2")


def get_all_documents_from_chroma_db(query: QueryData) -> list:
    """
        # Tool: Google search tool
    """
    # search = GoogleSearchAPIWrapper()
    # tool = Tool(
    #     name="google_search",
    #     description="Search Google for recent results.",
    #     func=search.run,
    # )

    # Google search each mutual fund summary
    all_files_data = []
    splitter = RecursiveJsonSplitter(max_chunk_size=5000)

    for file in query.asset_list:
        # Get the asset file from S3
        loader = JSONLoaderExtended(json_data=json.dumps(aws_helper.get_json_from_s3(settings.CONTABO_DATA_BUCKET,
                                                                                     f'{settings.MUTUAL_FUNDS_FUNDAMENTAL_DATA_PREFIX}{file}')),
                                    jq_schema=".", json_lines=False, text_content=False)
        docs = loader.load()
        data = json.loads(docs[0].page_content)
        # output = tool.run(f"Mutual fund {os.path.splitext(file)[0]} summary")
        # logger.info(output)
        # data['latest_summary'] = output

        # texts = splitter.split_text(json_data=data)
        docs = splitter.create_documents(texts=[data])
        # [logger.info(doc.page_content) for doc in docs]
        # all_files_data[file] = []
        for _, doc in enumerate(docs):
            # logger.info(doc.page_content)
            json_dict = json.loads(doc.page_content)
            json_dict['code'] = file
            all_files_data.append(
                Document(page_content=json.dumps(json_dict), metadata=doc.metadata))

    return all_files_data


def add_key_value_to_all_dicts(key_to_add: str, value_to_add: str, data: dict):
    """
    Recursively add key:value to all the dictionaries in the given dictionary
    """
    if isinstance(data, dict):
        data[key_to_add] = value_to_add
        for _, value in data.items():
            add_key_value_to_all_dicts(key_to_add, value_to_add, value)
    elif isinstance(data, list):
        for item in data:
            add_key_value_to_all_dicts(key_to_add, value_to_add, item)


def find_key_recursively(data, target_key):
    """
    Recursively search for a specific key in a nested JSON object.

    :param data: The JSON object (as a dictionary or list).
    :param target_key: The key to search for.
    :return: A list of values associated with the target key.
    """
    results = []

    if isinstance(data, dict):
        for key, value in data.items():
            if key == target_key:
                results.append(value)
            elif isinstance(value, (dict, list)):
                results.extend(find_key_recursively(value, target_key))
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                results.extend(find_key_recursively(item, target_key))

    return results


def get_relevant_documents(query: QueryData) -> dict:
    all_files_data = get_all_documents_from_chroma_db(query)
    relevant_docs = {}
    for doc in all_files_data:
        json_dict = json.loads(doc.page_content)
        if all([key not in doc.page_content for key in query.keys]):
            continue
        if json_dict["code"] in relevant_docs:
            relevant_docs[json_dict["code"]].update(json_dict)
        else:
            relevant_docs[json_dict["code"]] = json_dict

    filtered_relevant_documents = {}
    _ = [filtered_relevant_documents.update({key: []}) for key in query.keys]

    for query_key in query.keys:
        for relevant_document_key, value in relevant_docs.items():
            # Append code:filename.json recursively to all dictionaries
            add_key_value_to_all_dicts(
                "code", relevant_document_key, relevant_docs[relevant_document_key])
            value = find_key_recursively(value, query_key)
            for item in value:
                if isinstance(item, str) or isinstance(item, dict):
                    # If the item is a string, it means it is a value of the key
                    # We need to convert it to a dict with code as key
                    filtered_relevant_documents[query_key].append(
                        {"code": relevant_document_key, query_key: item})
                elif isinstance(item, list):
                    for inner_value in item:
                        if isinstance(inner_value, dict):
                            filtered_relevant_documents[query_key].append(
                                inner_value)

    if not filtered_relevant_documents:
        raise ValueError("Similarity search failed after %s tries. Search query: %s",
                         settings.MAX_SIMILARITY_SEARCH_COUNT, query)

    logger.info("Relevant documents: %s", filtered_relevant_documents)
    query.query = query.query.replace(
        "JSON", json.dumps(filtered_relevant_documents))
    return filtered_relevant_documents


def get_json_agent(llm: ChatOpenAI, queryData: QueryData):
    # # Agent: JSON Agent
    spec = JsonSpec(dict_=get_relevant_documents(
        queryData), max_value_length=4000)
    toolkit = JsonToolkitExtended(spec=spec)
    # verbose=True will automaticall invoke StdOutCallbackHandler
    handler = StdOutCallbackHandler()
    agent = create_json_agent(llm=llm,
                              toolkit=toolkit,
                              max_iterations=5,
                              verbose=True,
                              agent_executor_kwargs={
                                  'handle_parsing_errors': True},
                              return_intermediate_steps=True,
                              callbacks=[handler])
    return agent


def get_df_agent(llm: ChatOpenAI, query: dict):

    df = pd.read_csv(
        "https://raw.githubusercontent.com/pandas-dev/pandas/main/doc/data/titanic.csv"
    )

    def load_df_from_json_and_returns_html(self, tool_input: str) -> str:
        import pandas as pd
        logger.info(
            f"This is the tool input in pandas agent load_df_from_json_and_print: {tool_input}")
        logger.info(pd.DataFrame(json.loads(tool_input)))
        return pd.DataFrame(json.loads(tool_input)).to_html()

    def load_df_from_json(self, tool_input: str) -> str:
        import pandas as pd
        logger.info(
            f"This is the tool input in pandas agent load_df_from_json: {tool_input}")
        return pd.DataFrame(json.loads(tool_input))

    extra_tools = [PythonAstREPLTool(
        name="load_df_from_json",
        locals={"load_df_from_json": load_df_from_json},
        description="""
                This tool converts JSON string into a pandas dataframe and performs analysis on the pandas dataframe.
                Pass the provided JSON string to load_df_from_json function. 
                The function will return a pandas dataframe. 
                The return value of the function can be used to perform analysis.  
            """
    ), PythonAstREPLTool(
        name="load_df_from_json_and_returns_html",
        locals={
            "load_df_from_json_and_returns_html": load_df_from_json_and_returns_html},
        description="""
                This tool loads the provided JSON list into pandas dataframe and retruns the dataframe as html. 
                Call the function load_df_from_json_and_returns_html and provide the JSON string, 
                the function will return the output in the HTML format        
            """
    )]

    return create_pandas_dataframe_agent(llm, df, extra_tools=extra_tools, verbose=True)


def get_python_repl() -> PythonREPL:
    python_repl = PythonREPL()
    repl_tool = Tool(
        name="python_repl",
        description="""
        A Python shell. Use this to execute python commands. 
        Input should be a valid python command. If you want to see the output of a value, you should print it out with `print(...)`.
        You can use pandas library and perform analysis on pandas dataframe
        """,
        func=python_repl.run)
    return repl_tool


def get_llm() -> ChatOpenAI:
    return ChatOpenAI(model="gpt-4o")


def query_llm(query: dict):
    # Set global debug for detailed logging
    set_debug(False)

    # JSON Agent: Run agent and query
    with get_openai_callback() as cost:
        # Session 1
        # logger.info(agent.run("What is AAACX_US Top_Holdings Pennant Park Investment Corp weight"))
        # logger.info(agent.run("What are AAAAX_US Sector_Weights Names"))
        # logger.info(agent.run("What are common Sector_Weights between AAAAX_US and AAACX_US"))
        # response=agent({"input":"What are AAACX_US Top_Holdings and also get the latest summary and price"})
        # response=agent({"input":"List difference in Top_Holdings for AAACX_US and AAAAX_US"})

        # Session 2
        # logger.info(agent.run("For AAACX_US what is the value of  Prev_Close_Price"))
        # logger.info(agent.run("For AAACX_US in General2 what is the value of  Prev_Close_Price"))
        # logger.info(agent.run("What is AAACX_US Top_Holdings Pennant Park Investment Corp weight"))
        logger.info(get_json_agent(get_llm(), query).run(query["question"]))

        # logger.info cost
        logger.info(f"total tokens: {cost.total_tokens}")
        # Tokens in the question
        logger.info(f"prompt tokens: {cost.prompt_tokens}")
        # Tokens used to generated response
        logger.info(f"completion tokens: {cost.completion_tokens}")
        logger.info(f"cost is: {cost.total_cost}")
