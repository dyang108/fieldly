from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, BinaryIO, Optional, Protocol, Union, IO

from type_definitions import StorageConfig, FileInfo


class Storage(Protocol):
    """Protocol class for storage backends"""
    
    @property
    def config(self) -> StorageConfig:
        """Get the storage configuration"""
        ...


class StorageInterface(ABC):
    """Abstract interface for storage backends"""
    
    @property
    @abstractmethod
    def config(self) -> StorageConfig:
        """
        Get the storage configuration
        
        Returns:
            Dict with configuration values
        """
        pass
    
    @abstractmethod
    def save_file(self, dataset_name: str, file_obj: BinaryIO, filename: str) -> FileInfo:
        """
        Save a file to storage
        
        Args:
            dataset_name: Name of the dataset
            file_obj: File object to save
            filename: Name of the file
            
        Returns:
            Dict with file info
        """
        pass
    
    @abstractmethod
    def list_datasets(self) -> List[str]:
        """
        List all datasets in storage
        
        Returns:
            List of dataset names
        """
        pass
    
    @abstractmethod
    def list_files(self, dataset_name: str) -> List[FileInfo]:
        """
        List all files in a dataset
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            List of file info dicts with keys like 'key', 'size', 'last_modified'
        """
        pass
    
    @abstractmethod
    def get_file(self, dataset_name: str, filename: str) -> Optional[BinaryIO]:
        """
        Get a file from storage
        
        Args:
            dataset_name: Name of the dataset
            filename: Name of the file
            
        Returns:
            File-like object or None if not found
        """
        pass
    
    @abstractmethod
    def read_file(self, file_path: str) -> Optional[str]:
        """
        Read text file content from storage
        
        Args:
            file_path: Path to the file relative to storage root
            
        Returns:
            File content as string or None if file not found or cannot be read
        """
        pass
    
    @abstractmethod
    def delete_file(self, dataset_name: str, filename: str) -> bool:
        """
        Delete a file from storage
        
        Args:
            dataset_name: Name of the dataset
            filename: Name of the file
            
        Returns:
            True if successful, False otherwise
        """
        pass
    
    @abstractmethod
    def dataset_exists(self, dataset_name: str) -> bool:
        """
        Check if a dataset exists in storage
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            True if the dataset exists, False otherwise
        """
        pass
    
    @abstractmethod
    def create_dataset(self, dataset_name: str) -> bool:
        """
        Create a new dataset in storage
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            True if successful, False otherwise
        """
        pass 