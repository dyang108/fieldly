"""
Constants and configuration values used throughout the application.
"""

import os

# Default model name
DEFAULT_LOCAL_MODEL = os.getenv('OLLAMA_MODEL', 'deepseek-r1:14b')

# Default database configuration
DEFAULT_DATABASE_NAME = 'schemas.db'
DEFAULT_DATABASE_URL = f'sqlite:///{DEFAULT_DATABASE_NAME}'

# Default API URLs
DEFAULT_OLLAMA_API_URL = 'http://localhost:11434/api/chat'
DEFAULT_OLLAMA_HOST = 'http://localhost:11434'

# Model configuration
DEFAULT_LLM_PROVIDER = "deepseek"  # Options: "deepseek", "openai", "anthropic", "ollama", etc.

# Provider-specific configuration
PROVIDER_CONFIGS = {
    "deepseek": {
        "local": {
            "model": "deepseek-r1:14b",
            "api_url": "http://localhost:11434/api/chat",
        },
        "api": {
            "model": "deepseek-chat",
            "api_url": "https://api.deepseek.com/v1/chat/completions",
        }
    },
    "openai": {
        "api": {
            "model": "gpt-4-turbo-preview",
            "api_url": "https://api.openai.com/v1/chat/completions",
        }
    },
    "anthropic": {
        "api": {
            "model": "claude-3-opus-20240229",
            "api_url": "https://api.anthropic.com/v1/messages",
        }
    },
    "ollama": {
        "local": {
            "model": "llama3",
            "api_url": "http://localhost:11434/api/chat",
        }
    }
}

# Local model configurations
DEEPSEEK_LOCAL_CONFIG = {
    "model": DEFAULT_LOCAL_MODEL,
    "api_url": DEFAULT_OLLAMA_API_URL
}

# Default temperature settings
DEFAULT_TEMPERATURE = 0.3  # Lower for more deterministic extraction 