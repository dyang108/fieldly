import os
import glob
import logging
import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# List of supported file extensions
SUPPORTED_EXTENSIONS = {
    '.csv': 'CSV',
    '.json': 'JSON',
    '.xlsx': 'Excel',
    '.xls': 'Excel',
    '.txt': 'Text',
    '.md': 'Markdown',
    '.pdf': 'PDF',
}

def get_file_type(filename: str) -> str:
    """
    Determine the file type based on the file extension
    
    Args:
        filename: Path to the file
        
    Returns:
        str: File type description or 'Unknown'
    """
    _, ext = os.path.splitext(filename.lower())
    return SUPPORTED_EXTENSIONS.get(ext, 'Unknown')

def is_supported_file_type(filename: str) -> bool:
    """
    Check if the file type is supported for data extraction
    
    Args:
        filename: Path to the file
        
    Returns:
        bool: True if the file type is supported, False otherwise
    """
    _, ext = os.path.splitext(filename.lower())
    return ext in SUPPORTED_EXTENSIONS

def list_files_with_extensions(directory: str, extensions: List[str] = None) -> List[str]:
    """
    List files in a directory with specific extensions
    
    Args:
        directory: Directory path to search
        extensions: List of file extensions to include (without the dot)
        
    Returns:
        List[str]: List of file paths
    """
    if extensions is None:
        extensions = [ext.lstrip('.') for ext in SUPPORTED_EXTENSIONS.keys()]
    
    directory_path = Path(directory)
    
    if not directory_path.exists() or not directory_path.is_dir():
        logger.warning(f"Directory '{directory}' does not exist or is not a directory")
        return []
    
    files = []
    for ext in extensions:
        # Ensure the extension has a dot prefix
        if not ext.startswith('.'):
            ext = f'.{ext}'
        
        # Get all matching files
        pattern = f"*{ext}"
        matching_files = list(directory_path.glob(pattern))
        files.extend([str(f) for f in matching_files])
    
    logger.info(f"Found {len(files)} files with extensions {extensions} in '{directory}'")
    return sorted(files)

def read_file_as_text(file_path: str) -> Optional[str]:
    """
    Read a file and return its contents as text
    
    Args:
        file_path: Path to the file
        
    Returns:
        Optional[str]: File contents as text, or None if the file could not be read
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading file '{file_path}': {str(e)}")
        return None

def get_file_size(file_path: str) -> int:
    """
    Get the size of a file in bytes
    
    Args:
        file_path: Path to the file
        
    Returns:
        int: Size of the file in bytes, or 0 if the file does not exist
    """
    try:
        return os.path.getsize(file_path)
    except Exception as e:
        logger.error(f"Error getting size of file '{file_path}': {str(e)}")
        return 0 