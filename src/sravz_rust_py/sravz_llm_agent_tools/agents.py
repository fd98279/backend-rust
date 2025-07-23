from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent
import os
from . import tools, llm_settings

model = ChatOpenAI(model=llm_settings.LLM_MODEL)

# Create specialized agents
json_agent = create_react_agent(
    model=model,
    tools=[tools.get_value_a_key],
    name="json_expert",
    prompt="You are a json expert. Always use one tool at a time."
)

math_agent = create_react_agent(
    model=model,
    tools=[tools.add, tools.multiply],
    name="math_expert",
    prompt="You are a math expert. Always use one tool at a time."
)

save_to_file_agent = create_react_agent(
    model=model,
    tools=[tools.save_to_file],
    name="save_to_file_expert",
    prompt="You are a save_to_file expert. Always use one tool at a time."
)

save_to_contabo_and_get_presigned_url_agent = create_react_agent(
    model=model,
    tools=[tools.upload_file_to_contabo],
    name="save_to_contabo_and_get_presigned_url_expert",
    prompt="You are a save_to_contabo_and_get_presigned_url expert. Always use one tool at a time."
)

market_news_agent = create_react_agent(
    model=model,
    tools=[tools.get_market_news],
    name="market_news_expert",
    prompt="You are a market news expert. Always use one tool at a time."
)

research_agent = create_react_agent(
    model=model,
    tools=[tools.web_search],
    name="research_expert",
    prompt="You are a world class researcher with access to web search. Do not do any math. "
)
