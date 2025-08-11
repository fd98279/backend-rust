import os
LLM_MODEL = "gpt-4o"
REDIS_URI = f"redis://:{os.environ.get('REDIS_PASSWORD')}@{os.environ.get('REDIS_HOST')}:{os.environ.get('REDIS_PORT')}"
