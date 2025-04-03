"""
Common type definitions used across the codebase.
This file centralizes type definitions to avoid circular imports and redundancy.
"""

from typing import Dict, List, Any, Optional, Union, TypedDict, Literal

# LLM Provider types
Provider = Literal['deepseek', 'openai', 'anthropic', 'ollama']
StorageType = Literal['local', 's3']

# API configuration types
class ApiConfig(TypedDict, total=False):
    """Configuration for API providers"""
    model: str
    api_url: str
    
class ProviderModeConfig(TypedDict):
    """Configuration for a specific provider mode (local or API)"""
    local: Optional[ApiConfig]
    api: Optional[ApiConfig]
    
class ProviderConfigs(TypedDict, total=False):
    """Configuration for all providers"""
    deepseek: Dict[Literal['local', 'api'], ApiConfig]
    openai: Dict[Literal['api'], ApiConfig]
    anthropic: Dict[Literal['api'], ApiConfig]
    ollama: Dict[Literal['local'], ApiConfig]

# Storage configuration
class StorageConfig(TypedDict, total=False):
    """Storage configuration options"""
    bucket_name: Optional[str]
    aws_access_key_id: Optional[str]
    aws_secret_access_key: Optional[str]
    region_name: Optional[str]
    storage_path: Optional[str]

# File and extraction types
class FileInfo(TypedDict, total=False):
    """Information about a file in storage"""
    name: str
    path: str
    size: int
    last_modified: str

class FileResult(TypedDict, total=False):
    """Result of processing a single file"""
    filename: str
    status: Literal['success', 'error']
    output_file: Optional[str]
    message: Optional[str]

class ExtractorResponse(TypedDict, total=False):
    """Response for extraction endpoint"""
    success: bool
    error: Optional[str]
    dataset: Optional[str]
    output_directory: Optional[str]
    processed_files: Optional[int]
    results: Optional[List[FileResult]]

# Schema types
class SchemaProperty(TypedDict, total=False):
    """Property definition in JSON schema"""
    type: str
    description: Optional[str]
    format: Optional[str]
    enum: Optional[List[str]]
    
class SchemaDefinition(TypedDict, total=False):
    """JSON schema definition"""
    type: str
    properties: Dict[str, SchemaProperty]
    required: List[str]
    description: Optional[str] 