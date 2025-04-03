import os
import logging
from typing import Dict, Any, Optional

from .base import SchemaGenerator
from .model_api import APIModelSchemaGenerator
from .model_local import LocalOllamaSchemaGenerator
from .mock import MockSchemaGenerator
from .llm_extractor import LLMExtractor
from constants import DEFAULT_LLM_PROVIDER, PROVIDER_CONFIGS

logger = logging.getLogger(__name__)

def create_schema_generator(use_api: bool = False, api_key: Optional[str] = None, **kwargs) -> SchemaGenerator:
    """
    Factory function to create a schema generator based on configuration.
    
    Args:
        use_api: Whether to use API version (vs. local)
        api_key: API key for API access
        
    Returns:
        An instance of a SchemaGenerator implementation
    """
    if use_api:
        # Import here to avoid circular imports
        from .model_api import APIModelSchemaGenerator
        return APIModelSchemaGenerator(api_key=api_key, **kwargs)
    else:
        # Import here to avoid circular imports
        from .model_local import LocalOllamaSchemaGenerator
        return LocalOllamaSchemaGenerator(**kwargs)

def create_llm_extractor(use_api: bool = False, api_key: Optional[str] = None, 
                         provider: Optional[str] = None, **kwargs) -> LLMExtractor:
    """
    Factory function to create an LLM extractor based on configuration.
    
    Args:
        use_api: Whether to use API version (vs. local)
        api_key: API key for API access
        provider: LLM provider name (default from constants)
        
    Returns:
        An instance of the LLMExtractor
    """
    # Get provider from argument, environment variable, or default constant
    provider = provider or os.environ.get('LLM_PROVIDER') or DEFAULT_LLM_PROVIDER
    print(f"Provider: {provider}, os.environ.get('LLM_PROVIDER'): {os.environ.get('LLM_PROVIDER')}, DEFAULT_LLM_PROVIDER: {DEFAULT_LLM_PROVIDER}")

    # Create and return the extractor
    return LLMExtractor(use_api=use_api, api_key=api_key, provider=provider, **kwargs)

__all__ = [
    'SchemaGenerator', 
    'APIModelSchemaGenerator', 
    'LocalOllamaSchemaGenerator', 
    'MockSchemaGenerator',
    'create_schema_generator',
    'create_llm_extractor'
] 