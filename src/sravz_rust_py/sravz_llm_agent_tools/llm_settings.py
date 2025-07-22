import os

# Optional, add tracing in LangSmith
os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_PROJECT"] = "Sravz LLM"
os.environ["LANGCHAIN_API_KEY"] = "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"

LLM_MODEL = "gpt-4o"
