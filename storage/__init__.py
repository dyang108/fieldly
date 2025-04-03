"""
Storage module for Schema Generator.
This module provides storage backends for storing datasets and files.
"""

import logging
from typing import Dict, Any, Optional, cast, Union

from .base import StorageInterface, Storage
from .local import LocalStorage
from .s3 import S3Storage
from type_definitions import StorageType, StorageConfig

logger = logging.getLogger(__name__)

def create_storage(storage_type: StorageType, config: Optional[StorageConfig] = None) -> Storage:
    """
    Create a storage instance
    
    Args:
        storage_type: Type of storage ('local' or 's3')
        config: Configuration parameters
        
    Returns:
        Storage instance
    
    Raises:
        ValueError: If storage_type is invalid
    """
    if config is None:
        config = {}
        
    if storage_type == 'local':
        storage_path = config.get('storage_path', '.data')
        return LocalStorage(storage_path=storage_path)
    elif storage_type == 's3':
        return S3Storage(
            bucket_name=config.get('bucket_name', ''),
            aws_access_key_id=cast(str, config.get('aws_access_key_id', '')),
            aws_secret_access_key=cast(str, config.get('aws_secret_access_key', '')),
            region_name=cast(str, config.get('region_name', ''))
        )
    else:
        raise ValueError(f"Invalid storage type: {storage_type}")


__all__ = ['StorageInterface', 'LocalStorage', 'S3Storage', 'create_storage'] 