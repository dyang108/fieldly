import os
import pathlib
from datetime import datetime
from typing import List, Dict, Any, BinaryIO

from .base import StorageInterface


class LocalStorage(StorageInterface):
    """Local file system storage implementation"""
    
    def __init__(self, storage_path: str = '.data'):
        """
        Initialize local storage
        
        Args:
            storage_path: Path to store files, relative to current working directory
        """
        self.storage_path = os.path.join(os.getcwd(), storage_path)
        self.storage_dir = pathlib.Path(self.storage_path)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        # Store config as a private attribute
        self._config = {'storage_path': self.storage_path}
    
    @property
    def config(self) -> Dict[str, Any]:
        """
        Get the storage configuration
        
        Returns:
            Dict with configuration values
        """
        return self._config
    
    def save_file(self, dataset_name: str, file_obj: BinaryIO, filename: str) -> Dict[str, Any]:
        """
        Save a file to the local file system
        
        Args:
            dataset_name: Name of the dataset (subdirectory)
            file_obj: File object to save
            filename: Name of the file
            
        Returns:
            Dict with file info
        """
        # Create dataset directory if it doesn't exist
        dataset_dir = self.storage_dir / dataset_name
        dataset_dir.mkdir(exist_ok=True)
        
        # Save file
        file_path = dataset_dir / filename
        file_obj.save(file_path)
        
        return {
            'message': 'File uploaded successfully',
            'filename': filename,
            'path': str(file_path),
            'size': file_path.stat().st_size,
            'last_modified': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
        }
    
    def list_datasets(self) -> List[str]:
        """
        List all datasets in local storage
        
        Returns:
            List of dataset names (directory names)
        """
        if not self.storage_dir.exists():
            return []
        
        return [d.name for d in self.storage_dir.glob('*') if d.is_dir()]
    
    def list_files(self, dataset_name: str) -> List[Dict[str, Any]]:
        """
        List all files in a dataset
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            List of file info dicts
        """
        dataset_path = self.storage_dir / dataset_name
        
        if not dataset_path.exists() or not dataset_path.is_dir():
            return []
        
        files = []
        for file_path in dataset_path.glob('*'):
            if file_path.is_file():
                stat = file_path.stat()
                files.append({
                    'key': os.path.join(dataset_name, file_path.name),
                    'size': stat.st_size,
                    'last_modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    'name': file_path.name
                })
        
        return files
    
    def get_file(self, dataset_name: str, filename: str) -> Any:
        """
        Get a file from storage
        
        Args:
            dataset_name: Name of the dataset
            filename: Name of the file
            
        Returns:
            File path object
        """
        file_path = self.storage_dir / dataset_name / filename
        
        if not file_path.exists() or not file_path.is_file():
            return None
        
        return file_path
    
    def delete_file(self, dataset_name: str, filename: str) -> bool:
        """
        Delete a file from storage
        
        Args:
            dataset_name: Name of the dataset
            filename: Name of the file
            
        Returns:
            True if successful, False otherwise
        """
        file_path = self.storage_dir / dataset_name / filename
        
        if not file_path.exists() or not file_path.is_file():
            return False
        
        try:
            file_path.unlink()
            return True
        except Exception:
            return False
            
    def dataset_exists(self, dataset_name: str) -> bool:
        """
        Check if a dataset exists in storage
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            True if the dataset exists, False otherwise
        """
        dataset_path = self.storage_dir / dataset_name
        return dataset_path.exists() and dataset_path.is_dir()
        
    def create_dataset(self, dataset_name: str) -> bool:
        """
        Create a new dataset in storage
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            True if successful, False otherwise
        """
        try:
            dataset_path = self.storage_dir / dataset_name
            dataset_path.mkdir(exist_ok=True, parents=True)
            return True
        except Exception:
            return False 