"""
Constants and configuration values used throughout the application.
This file provides a simplified interface to the configuration singleton.
"""

from config import config

# Storage Configuration
STORAGE_TYPE = config.STORAGE_TYPE
LOCAL_STORAGE_PATH = config.LOCAL_STORAGE_PATH

# S3 Configuration
S3_BUCKET_NAME = config.S3_BUCKET_NAME
AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
AWS_REGION = config.AWS_REGION

# AI Configuration
USE_LOCAL_MODEL = config.USE_LOCAL_MODEL
LLM_PROVIDER = config.LLM_PROVIDER
DEFAULT_LLM_PROVIDER = "deepseek"  # Options: "deepseek", "openai", "anthropic", "ollama"

# Local Model Configuration
OLLAMA_MODEL = config.OLLAMA_MODEL
DEFAULT_LOCAL_MODEL = OLLAMA_MODEL  # For backward compatibility
OLLAMA_HOST = config.OLLAMA_HOST
DEFAULT_OLLAMA_HOST = OLLAMA_HOST  # For backward compatibility
OLLAMA_API_PATH = config.OLLAMA_API_PATH
DEFAULT_OLLAMA_API_PATH = OLLAMA_API_PATH  # For backward compatibility
OLLAMA_API_URL = config.OLLAMA_API_URL
DEFAULT_OLLAMA_API_URL = OLLAMA_API_URL  # For backward compatibility

# API Configuration
DEEPSEEK_API_KEY = config.DEEPSEEK_API_KEY
DEEPSEEK_API_URL = config.DEEPSEEK_API_URL

OPENAI_API_KEY = config.OPENAI_API_KEY
OPENAI_API_URL = config.OPENAI_API_URL

ANTHROPIC_API_KEY = config.ANTHROPIC_API_KEY
ANTHROPIC_API_URL = config.ANTHROPIC_API_URL

# Database Configuration
DATABASE_NAME = config.DATABASE_NAME
DEFAULT_DATABASE_NAME = 'schemas.db'  # Default database name
DATABASE_URL = config.DATABASE_URL
DEFAULT_DATABASE_URL = f'sqlite:///{DEFAULT_DATABASE_NAME}'  # For backward compatibility

# Flask Configuration
SECRET_KEY = config.SECRET_KEY
DEBUG = config.DEBUG

# Provider-specific configuration
PROVIDER_CONFIGS = config.PROVIDER_CONFIGS
MODEL_CONFIGS = config.MODEL_CONFIGS

# Default temperature settings
DEFAULT_TEMPERATURE = config.DEFAULT_TEMPERATURE
DEFAULT_MAX_TOKENS = config.DEFAULT_MAX_TOKENS

# Validation constants
MAX_CHUNK_SIZE = config.MAX_CHUNK_SIZE
MIN_CHUNK_SIZE = config.MIN_CHUNK_SIZE 