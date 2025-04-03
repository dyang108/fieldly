"""
Configuration management for the application.
This module provides a single source of truth for all configuration values.
"""

import os
from typing import Dict, Any, Literal, Union
from dotenv import load_dotenv

class Config:
    """Application configuration"""
    
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Storage Configuration
        self.STORAGE_TYPE: Literal['local', 's3'] = os.getenv('STORAGE_TYPE', 'local')
        self.LOCAL_STORAGE_PATH: str = os.getenv('LOCAL_STORAGE_PATH', '.data')
        
        # S3 Configuration
        self.S3_BUCKET_NAME: str = os.getenv('S3_BUCKET_NAME', '')
        self.AWS_ACCESS_KEY_ID: str = os.getenv('AWS_ACCESS_KEY_ID', '')
        self.AWS_SECRET_ACCESS_KEY: str = os.getenv('AWS_SECRET_ACCESS_KEY', '')
        self.AWS_REGION: str = os.getenv('AWS_REGION', 'us-west-1')
        
        # AI Configuration
        self.USE_LOCAL_MODEL: bool = os.getenv('USE_LOCAL_MODEL', 'true').lower() == 'true'
        self.LLM_PROVIDER: Literal['deepseek', 'openai', 'anthropic', 'ollama'] = os.getenv('LLM_PROVIDER', 'deepseek')
        
        # Local Model Configuration
        self.OLLAMA_MODEL: str = os.getenv('OLLAMA_MODEL', 'deepseek-r1:14b')
        self.OLLAMA_HOST: str = os.getenv('OLLAMA_HOST', 'http://localhost:11434')
        self.OLLAMA_API_PATH: str = os.getenv('OLLAMA_API_PATH', '/api/chat')
        self.OLLAMA_API_URL: str = f"{self.OLLAMA_HOST}{self.OLLAMA_API_PATH}"
        
        # API Configuration
        self.DEEPSEEK_API_KEY: str = os.getenv('DEEPSEEK_API_KEY', '')
        self.DEEPSEEK_API_URL: str = os.getenv('DEEPSEEK_API_URL', 'https://api.deepseek.com/v1/chat/completions')
        
        self.OPENAI_API_KEY: str = os.getenv('OPENAI_API_KEY', '')
        self.OPENAI_API_URL: str = os.getenv('OPENAI_API_URL', 'https://api.openai.com/v1/chat/completions')
        
        self.ANTHROPIC_API_KEY: str = os.getenv('ANTHROPIC_API_KEY', '')
        self.ANTHROPIC_API_URL: str = os.getenv('ANTHROPIC_API_URL', 'https://api.anthropic.com/v1/messages')
        
        # Database Configuration
        self.DATABASE_NAME: str = os.getenv('DATABASE_NAME', 'schemas.db')
        self.DATABASE_URL: str = os.getenv('DATABASE_URL', f'sqlite:///{self.DATABASE_NAME}')
        
        # Flask Configuration
        self.SECRET_KEY: str = os.getenv('SECRET_KEY', 'dev')
        self.DEBUG: bool = os.getenv('DEBUG', 'false').lower() == 'true'
        
        # Provider-specific configuration
        self.PROVIDER_CONFIGS: Dict[str, Dict[str, Dict[str, str]]] = {
            "deepseek": {
                "api": {
                    "model": "deepseek-chat",
                    "api_url": self.DEEPSEEK_API_URL
                }
            },
            "openai": {
                "api": {
                    "model": "gpt-4-turbo-preview",
                    "api_url": self.OPENAI_API_URL
                }
            },
            "anthropic": {
                "api": {
                    "model": "claude-3-opus-20240229",
                    "api_url": self.ANTHROPIC_API_URL
                }
            },
            "ollama": {
                "local": {
                    "model": self.OLLAMA_MODEL,
                    "api_url": self.OLLAMA_API_URL
                },
            }
        }
        
        # Model configuration (simplified version of PROVIDER_CONFIGS)
        self.MODEL_CONFIGS: Dict[str, Dict[str, Dict[str, str]]] = {
            "deepseek": {
                "local": {
                    "model": self.OLLAMA_MODEL,
                    "api_url": self.OLLAMA_API_URL
                },
                "api": {
                    "api_url": self.DEEPSEEK_API_URL
                }
            },
            "openai": {
                "api": {
                    "api_url": self.OPENAI_API_URL
                }
            },
            "anthropic": {
                "api": {
                    "api_url": self.ANTHROPIC_API_URL
                }
            },
            "ollama": {
                "local": {
                    "api_url": self.OLLAMA_API_URL
                }
            }
        }
        
        # Default settings
        self.DEFAULT_TEMPERATURE: float = 0.3
        self.DEFAULT_MAX_TOKENS: int = 4000
        self.MAX_CHUNK_SIZE: int = 4000
        self.MIN_CHUNK_SIZE: int = 100

# Create a singleton instance
config = Config() 