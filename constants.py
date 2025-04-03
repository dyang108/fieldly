"""
Constants and configuration values used throughout the application.
This file provides a simplified interface to the configuration singleton.
"""

from typing import Dict, Any, Optional

from config import config
from type_definitions import Provider, StorageType, ProviderConfigs

# Storage Configuration
STORAGE_TYPE: StorageType = config.STORAGE_TYPE
LOCAL_STORAGE_PATH: str = config.LOCAL_STORAGE_PATH

# S3 Configuration
S3_BUCKET_NAME: str = config.S3_BUCKET_NAME
AWS_ACCESS_KEY_ID: str = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY: str = config.AWS_SECRET_ACCESS_KEY
AWS_REGION: str = config.AWS_REGION

# AI Configuration
USE_LOCAL_MODEL: bool = config.USE_LOCAL_MODEL
LLM_PROVIDER: Provider = config.LLM_PROVIDER
DEFAULT_LLM_PROVIDER: Provider = "deepseek"

# Local Model Configuration
OLLAMA_MODEL: str = config.OLLAMA_MODEL
DEFAULT_LOCAL_MODEL: str = OLLAMA_MODEL  # For backward compatibility
OLLAMA_HOST: str = config.OLLAMA_HOST
DEFAULT_OLLAMA_HOST: str = OLLAMA_HOST  # For backward compatibility
OLLAMA_API_PATH: str = config.OLLAMA_API_PATH
DEFAULT_OLLAMA_API_PATH: str = OLLAMA_API_PATH  # For backward compatibility
OLLAMA_API_URL: str = config.OLLAMA_API_URL
DEFAULT_OLLAMA_API_URL: str = OLLAMA_API_URL  # For backward compatibility

# API Configuration
DEEPSEEK_API_KEY: str = config.DEEPSEEK_API_KEY
DEEPSEEK_API_URL: str = config.DEEPSEEK_API_URL

OPENAI_API_KEY: str = config.OPENAI_API_KEY
OPENAI_API_URL: str = config.OPENAI_API_URL

ANTHROPIC_API_KEY: str = config.ANTHROPIC_API_KEY
ANTHROPIC_API_URL: str = config.ANTHROPIC_API_URL

# Database Configuration
DATABASE_NAME: str = config.DATABASE_NAME
DEFAULT_DATABASE_NAME: str = 'schemas.db'  # Default database name
DATABASE_URL: str = config.DATABASE_URL
DEFAULT_DATABASE_URL: str = f'sqlite:///{DEFAULT_DATABASE_NAME}'  # For backward compatibility

# Flask Configuration
SECRET_KEY: str = config.SECRET_KEY
DEBUG: bool = config.DEBUG

# Provider-specific configuration
PROVIDER_CONFIGS: ProviderConfigs = config.PROVIDER_CONFIGS
MODEL_CONFIGS: ProviderConfigs = config.MODEL_CONFIGS

# Default temperature settings
DEFAULT_TEMPERATURE: float = config.DEFAULT_TEMPERATURE
DEFAULT_MAX_TOKENS: int = config.DEFAULT_MAX_TOKENS

# Validation constants
MAX_CHUNK_SIZE: int = config.MAX_CHUNK_SIZE
MIN_CHUNK_SIZE: int = config.MIN_CHUNK_SIZE 