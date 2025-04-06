import os
import logging
from typing import Dict, Any, Optional

from .base import SchemaGenerator
from .model_api import APIModelSchemaGenerator
from .model_local import LocalOllamaSchemaGenerator
from .mock import MockSchemaGenerator
from .llm_extractor import LLMExtractor
from constants import DEFAULT_LLM_PROVIDER, PROVIDER_CONFIGS, DEFAULT_LOCAL_MODEL, DEFAULT_OLLAMA_API_URL

logger = logging.getLogger(__name__)

def create_schema_generator(
    use_local_model: bool = True,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    api_url: Optional[str] = None,
    **kwargs
) -> SchemaGenerator:
    """
    Factory function to create a schema generator based on configuration.
    
    Args:
        use_local_model: Whether to use local model (True) or API model (False)
        api_key: API key for API access (required if use_local_model is False)
        model: Model name to use (defaults to DEFAULT_LOCAL_MODEL for local, required for API)
        api_url: API URL to use (defaults to DEFAULT_OLLAMA_API_URL for local, required for API)
        **kwargs: Additional arguments passed to the generator
        
    Returns:
        An instance of a SchemaGenerator implementation
        
    Raises:
        ValueError: If required parameters are missing
    """
    if use_local_model:
        # Import here to avoid circular imports
        from .model_local import LocalOllamaSchemaGenerator
        return LocalOllamaSchemaGenerator(
            model=model or DEFAULT_LOCAL_MODEL,
            api_url=api_url or DEFAULT_OLLAMA_API_URL,
            **kwargs
        )
    else:
        if not api_key:
            raise ValueError("api_key is required when using API model")
        if not model:
            raise ValueError("model is required when using API model")
        if not api_url:
            raise ValueError("api_url is required when using API model")
            
        # Import here to avoid circular imports
        from .model_api import APIModelSchemaGenerator
        return APIModelSchemaGenerator(
            api_key=api_key,
            api_url=api_url,
            model_name=model,
            **kwargs
        )

def create_llm_extractor(use_api: bool = False, api_key: Optional[str] = None, 
                         provider: Optional[str] = None, **kwargs) -> LLMExtractor:
    """
    Create an LLM extractor with the specified configuration
    
    Args:
        use_api: Whether to use an API or local model
        api_key: API key for the provider
        provider: LLM provider name (default from constants)
        **kwargs: Additional configuration parameters
        
    Returns:
        An instance of the LLMExtractor
    """
    # Check if the first argument is a dictionary (for backward compatibility)
    if isinstance(use_api, dict):
        config = use_api
        use_api = config.get('use_api', False)
        api_key = config.get('api_key', None)
        provider = config.get('provider', None)
        # Add any other config parameters to kwargs
        for key, value in config.items():
            if key not in ['use_api', 'api_key', 'provider']:
                kwargs[key] = value
    
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