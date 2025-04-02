from typing import Dict, Any, Optional

from .base import StorageInterface
from .local import LocalStorage
from .s3 import S3Storage


def create_storage(storage_type: str, config: Optional[Dict[str, Any]] = None) -> StorageInterface:
    """
    Create a storage instance based on type and configuration
    
    Args:
        storage_type: 'local' or 's3'
        config: Configuration dict with storage-specific parameters
        
    Returns:
        Storage instance implementing StorageInterface
        
    Raises:
        ValueError: If storage_type is not supported
    """
    if config is None:
        config = {}
    
    if storage_type.lower() == 'local':
        storage_path = config.get('storage_path', '.data')
        return LocalStorage(storage_path=storage_path)
    
    elif storage_type.lower() == 's3':
        if 'bucket_name' not in config:
            raise ValueError("S3 storage requires 'bucket_name' in config")
            
        return S3Storage(
            bucket_name=config['bucket_name'],
            aws_access_key_id=config.get('aws_access_key_id'),
            aws_secret_access_key=config.get('aws_secret_access_key'),
            region_name=config.get('region_name')
        )
    
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")


__all__ = ['StorageInterface', 'LocalStorage', 'S3Storage', 'create_storage'] 