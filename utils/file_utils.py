import os
import glob
import logging
import json
import csv
import pandas as pd
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

def create_directory_if_not_exists(directory: str) -> bool:
    """
    Create a directory if it doesn't exist
    
    Args:
        directory: Directory path to create
        
    Returns:
        bool: True if directory exists or was created, False otherwise
    """
    try:
        os.makedirs(directory, exist_ok=True)
        return True
    except Exception as e:
        logger.error(f"Error creating directory '{directory}': {str(e)}")
        return False

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

def read_json_file(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Read a JSON file and return its contents as a dictionary
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Optional[Dict[str, Any]]: JSON data as a dictionary, or None if the file could not be read
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error reading JSON file '{file_path}': {str(e)}")
        return None

def write_json_file(file_path: str, data: Dict[str, Any], indent: int = 2) -> bool:
    """
    Write a dictionary to a JSON file
    
    Args:
        file_path: Path to the JSON file
        data: Dictionary to write
        indent: Indentation level for the JSON file
        
    Returns:
        bool: True if the file was written successfully, False otherwise
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent)
        return True
    except Exception as e:
        logger.error(f"Error writing JSON file '{file_path}': {str(e)}")
        return False

def read_csv_file(file_path: str) -> Optional[List[Dict[str, Any]]]:
    """
    Read a CSV file and return its contents as a list of dictionaries
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Optional[List[Dict[str, Any]]]: CSV data as a list of dictionaries, or None if the file could not be read
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as e:
        logger.error(f"Error reading CSV file '{file_path}': {str(e)}")
        return None

def write_csv_file(file_path: str, data: List[Dict[str, Any]], fieldnames: Optional[List[str]] = None) -> bool:
    """
    Write a list of dictionaries to a CSV file
    
    Args:
        file_path: Path to the CSV file
        data: List of dictionaries to write
        fieldnames: List of field names (column headers) to include
        
    Returns:
        bool: True if the file was written successfully, False otherwise
    """
    try:
        # If fieldnames are not provided, get them from the first dictionary
        if fieldnames is None and data:
            fieldnames = list(data[0].keys())
        
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)
        return True
    except Exception as e:
        logger.error(f"Error writing CSV file '{file_path}': {str(e)}")
        return False

def read_excel_file(file_path: str, sheet_name: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
    """
    Read an Excel file and return its contents as a list of dictionaries
    
    Args:
        file_path: Path to the Excel file
        sheet_name: Name of the sheet to read (defaults to first sheet)
        
    Returns:
        Optional[List[Dict[str, Any]]]: Excel data as a list of dictionaries, or None if the file could not be read
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Error reading Excel file '{file_path}': {str(e)}")
        return None

def write_excel_file(file_path: str, data: List[Dict[str, Any]], sheet_name: str = 'Sheet1') -> bool:
    """
    Write a list of dictionaries to an Excel file
    
    Args:
        file_path: Path to the Excel file
        data: List of dictionaries to write
        sheet_name: Name of the sheet to write
        
    Returns:
        bool: True if the file was written successfully, False otherwise
    """
    try:
        df = pd.DataFrame(data)
        df.to_excel(file_path, sheet_name=sheet_name, index=False)
        return True
    except Exception as e:
        logger.error(f"Error writing Excel file '{file_path}': {str(e)}")
        return False

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

def get_file_info(file_path: str) -> Dict[str, Any]:
    """
    Get information about a file
    
    Args:
        file_path: Path to the file
        
    Returns:
        Dict[str, Any]: Dictionary with file information
    """
    try:
        stat = os.stat(file_path)
        return {
            'path': file_path,
            'name': os.path.basename(file_path),
            'size': stat.st_size,
            'modified': stat.st_mtime,
            'type': get_file_type(file_path)
        }
    except Exception as e:
        logger.error(f"Error getting info for file '{file_path}': {str(e)}")
        return {
            'path': file_path,
            'name': os.path.basename(file_path),
            'size': 0,
            'modified': 0,
            'type': 'Unknown'
        } 