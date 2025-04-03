"""
Configuration interface for the application.
This file provides type-safe access to environment variables and configuration values.
"""

import os
from typing import Literal, Optional
from constants import (
    MODEL_CONFIGS,
    DEFAULT_DATABASE_NAME,
    DEFAULT_STORAGE_PATH,
    DEFAULT_OLLAMA_HOST,
    DEFAULT_OLLAMA_API_PATH
)

# Storage configuration
STORAGE_TYPE: Literal['local', 's3'] = os.getenv('STORAGE_TYPE', 'local')
LOCAL_STORAGE_PATH: str = os.getenv('LOCAL_STORAGE_PATH', DEFAULT_STORAGE_PATH)

# S3 configuration (only used if STORAGE_TYPE='s3')
S3_BUCKET_NAME: Optional[str] = os.getenv('S3_BUCKET_NAME')
AWS_ACCESS_KEY_ID: Optional[str] = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION: Optional[str] = os.getenv('AWS_REGION', 'us-west-1')

# AI configuration
USE_LOCAL_MODEL: bool = os.getenv('USE_LOCAL_MODEL', 'true').lower() == 'true'
LLM_PROVIDER: Literal['deepseek', 'openai', 'anthropic', 'ollama'] = os.getenv('LLM_PROVIDER', 'deepseek')

# Model configuration
def get_model_config() -> dict:
    """Get the configuration for the current model and mode."""
    mode = 'local' if USE_LOCAL_MODEL else 'api'
    return MODEL_CONFIGS[LLM_PROVIDER][mode]

# Local model configuration
OLLAMA_MODEL: str = os.getenv('OLLAMA_MODEL', get_model_config().get('model', 'deepseek-r1:14b'))
OLLAMA_API_URL: str = os.getenv('OLLAMA_API_URL', f"{DEFAULT_OLLAMA_HOST}{DEFAULT_OLLAMA_API_PATH}")

# API configurations (only used if USE_LOCAL_MODEL=false)
DEEPSEEK_API_KEY: Optional[str] = os.getenv('DEEPSEEK_API_KEY')
DEEPSEEK_API_URL: str = os.getenv('DEEPSEEK_API_URL', MODEL_CONFIGS['deepseek']['api']['api_url'])

OPENAI_API_KEY: Optional[str] = os.getenv('OPENAI_API_KEY')
OPENAI_API_URL: str = os.getenv('OPENAI_API_URL', MODEL_CONFIGS['openai']['api']['api_url'])

ANTHROPIC_API_KEY: Optional[str] = os.getenv('ANTHROPIC_API_KEY')
ANTHROPIC_API_URL: str = os.getenv('ANTHROPIC_API_URL', MODEL_CONFIGS['anthropic']['api']['api_url'])

# Database configuration
DATABASE_URL: str = os.getenv('DATABASE_URL', f'sqlite:///{DEFAULT_DATABASE_NAME}')
SQLALCHEMY_DATABASE_URI: str = DATABASE_URL
SQLALCHEMY_TRACK_MODIFICATIONS: bool = False

# Flask configuration
SECRET_KEY: str = os.getenv('SECRET_KEY', 'dev')
DEBUG: bool = os.getenv('DEBUG', 'false').lower() == 'true' 