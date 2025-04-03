"""
Configuration template for the application.
Copy this file to config.py and update the values as needed.
"""

# Storage configuration
STORAGE_TYPE = 'local'  # 'local' or 's3'
LOCAL_STORAGE_PATH = '.data'

# S3 configuration (if using S3 storage)
S3_BUCKET_NAME = ''
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_REGION = ''

# LLM configuration
USE_API = 'false'  # 'true' to use cloud APIs, 'false' for local models
LLM_PROVIDER = 'deepseek'  # 'deepseek', 'openai', 'anthropic', 'ollama', etc.

# Provider-specific configurations

# DeepSeek
DEEPSEEK_API_KEY = ''
DEEPSEEK_API_URL = 'https://api.deepseek.com/v1/chat/completions'
DEEPSEEK_LOCAL_MODEL = 'deepseek-r1:14b'
DEEPSEEK_LOCAL_API_URL = 'http://localhost:11434/api/chat'

# OpenAI
OPENAI_API_KEY = ''
OPENAI_API_URL = 'https://api.openai.com/v1/chat/completions'

# Anthropic
ANTHROPIC_API_KEY = ''
ANTHROPIC_API_URL = 'https://api.anthropic.com/v1/messages'

# Ollama
OLLAMA_LOCAL_MODEL = 'llama3'
OLLAMA_LOCAL_API_URL = 'http://localhost:11434/api/chat'

# Database configuration
SQLALCHEMY_DATABASE_URI = 'sqlite:///app.db'
SQLALCHEMY_TRACK_MODIFICATIONS = False

# Flask configuration
SECRET_KEY = 'generate-a-secure-key'
DEBUG = True 