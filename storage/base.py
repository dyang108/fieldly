from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any


class StorageInterface(ABC):
    """Abstract interface for storage backends"""
    
    @abstractmethod
    def save_file(self, dataset_name: str, file_obj: Any, filename: str) -> Dict[str, Any]:
        """
        Save a file to storage
        
        Args:
            dataset_name: Name of the dataset
            file_obj: File-like object to save
            filename: Name of the file
            
        Returns:
            Dict with file info including path, name, etc.
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
    def list_files(self, dataset_name: str) -> List[Dict[str, Any]]:
        """
        List all files in a dataset
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            List of file info dicts with keys like 'key', 'size', 'last_modified'
        """
        pass
    
    @abstractmethod
    def get_file(self, dataset_name: str, filename: str) -> Any:
        """
        Get a file from storage
        
        Args:
            dataset_name: Name of the dataset
            filename: Name of the file
            
        Returns:
            File-like object
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