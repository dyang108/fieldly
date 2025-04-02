from typing import Dict, Any, Optional

from .base import SchemaGenerator
from .deepseek_api import DeepSeekAPIGenerator
from .deepseek_local import DeepSeekLocalGenerator
from .mock import MockSchemaGenerator


def create_schema_generator(generator_type: str, config: Optional[Dict[str, Any]] = None) -> SchemaGenerator:
    """
    Create a schema generator instance based on type and configuration
    
    Args:
        generator_type: 'deepseek_api', 'deepseek_local', or 'mock'
        config: Configuration dict with generator-specific parameters
        
    Returns:
        SchemaGenerator instance
        
    Raises:
        ValueError: If generator_type is not supported
    """
    if config is None:
        config = {}
    
    if generator_type.lower() == 'deepseek_api':
        if 'api_key' not in config:
            raise ValueError("DeepSeek API requires 'api_key' in config")
            
        return DeepSeekAPIGenerator(
            api_key=config['api_key'],
            api_url=config.get('api_url', "https://api.deepseek.com/v1/chat/completions")
        )
    
    elif generator_type.lower() == 'deepseek_local':
        return DeepSeekLocalGenerator(
            model=config.get('model', 'deepseek-r1:14b'),
            api_url=config.get('api_url', "http://localhost:11434/api/chat")
        )
    
    elif generator_type.lower() == 'mock':
        return MockSchemaGenerator()
    
    else:
        raise ValueError(f"Unsupported schema generator type: {generator_type}")


__all__ = [
    'SchemaGenerator', 
    'DeepSeekAPIGenerator', 
    'DeepSeekLocalGenerator', 
    'MockSchemaGenerator',
    'create_schema_generator'
] 