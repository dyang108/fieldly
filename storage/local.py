import os
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, BinaryIO, Optional, cast

from .base import StorageInterface
from type_definitions import FileInfo, StorageConfig

logger = logging.getLogger(__name__)


class LocalStorage(StorageInterface):
    """Local filesystem storage backend"""
    
    def __init__(self, storage_path: str = '.data'):
        """
        Initialize local storage
        
        Args:
            storage_path: Path to the storage directory
        """
        self._storage_path = storage_path
        self._config: StorageConfig = {'storage_path': storage_path}
        
        # Create storage directory if it doesn't exist
        os.makedirs(self._storage_path, exist_ok=True)
    
    @property
    def config(self) -> StorageConfig:
        """Get storage configuration"""
        return self._config
    
    def save_file(self, dataset_name: str, file_obj: BinaryIO, filename: str) -> FileInfo:
        """
        Save a file to local storage
        
        Args:
            dataset_name: Name of the dataset
            file_obj: File object to save
            filename: Name of the file
            
        Returns:
            Dict with file info
        """
        # Create dataset directory if it doesn't exist
        dataset_path = Path(self._storage_path) / dataset_name
        os.makedirs(dataset_path, exist_ok=True)
        
        # Save the file
        file_path = dataset_path / filename
        with open(file_path, 'wb') as f:
            file_obj.seek(0)
            shutil.copyfileobj(file_obj, f)
        
        # Return file info
        return {
            'name': filename,
            'path': str(file_path),
            'size': os.path.getsize(file_path),
            'last_modified': datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
        }
    
    def list_datasets(self) -> List[str]:
        """
        List all datasets in local storage
        
        Returns:
            List of dataset names
        """
        datasets = []
        storage_path = Path(self._storage_path)
        
        if not storage_path.exists():
            return []
            
        for item in storage_path.iterdir():
            if item.is_dir() and not item.name.startswith('.'):
                datasets.append(item.name)
                
        return datasets
    
    def list_files(self, dataset_name: str) -> List[FileInfo]:
        """
        List all files in a dataset
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            List of file info dicts
        """
        files: List[FileInfo] = []
        dataset_path = Path(self._storage_path) / dataset_name
        
        if not dataset_path.exists():
            return []
            
        for item in dataset_path.iterdir():
            if item.is_file():
                files.append({
                    'name': item.name,
                    'path': str(item),
                    'size': item.stat().st_size,
                    'last_modified': datetime.fromtimestamp(item.stat().st_mtime).isoformat()
                })
                
        return files
    
    def get_file(self, dataset_name: str, filename: str) -> Optional[BinaryIO]:
        """
        Get a file from local storage
        
        Args:
            dataset_name: Name of the dataset
            filename: Name of the file
            
        Returns:
            File object or None if the file doesn't exist
        """
        file_path = Path(self._storage_path) / dataset_name / filename
        
        if not file_path.exists():
            return None
            
        return open(file_path, 'rb')
    
    def read_file(self, file_path: str) -> Optional[str]:
        """
        Read text file content from storage
        
        Args:
            file_path: Path to the file relative to storage root
            
        Returns:
            File content as string or None if file not found or cannot be read
        """
        full_path = Path(self._storage_path) / file_path
        
        if not full_path.exists():
            return None
            
        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            return None
    
    def delete_file(self, dataset_name: str, filename: str) -> bool:
        """
        Delete a file from local storage
        
        Args:
            dataset_name: Name of the dataset
            filename: Name of the file
            
        Returns:
            True if successful, False otherwise
        """
        file_path = Path(self._storage_path) / dataset_name / filename
        
        if not file_path.exists():
            return False
            
        try:
            os.remove(file_path)
            return True
        except Exception as e:
            logger.error(f"Error deleting file {filename}: {str(e)}")
            return False
    
    def dataset_exists(self, dataset_name: str) -> bool:
        """
        Check if a dataset exists in local storage
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            True if the dataset exists, False otherwise
        """
        dataset_path = Path(self._storage_path) / dataset_name
        return dataset_path.exists() and dataset_path.is_dir()
    
    def create_dataset(self, dataset_name: str) -> bool:
        """
        Create a new dataset in local storage
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            True if successful, False otherwise
        """
        dataset_path = Path(self._storage_path) / dataset_name
        
        if dataset_path.exists():
            return True
            
        try:
            os.makedirs(dataset_path, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"Error creating dataset {dataset_name}: {str(e)}")
            return False 