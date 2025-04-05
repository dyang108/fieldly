import logging
import json
import csv
import pandas as pd
import os
from typing import Dict, Any, List, Optional, Set, Tuple
from pathlib import Path

# Import local utilities
from .file_utils import get_file_type, is_supported_file_type

logger = logging.getLogger(__name__)

def detect_field_type(value: Any) -> str:
    """
    Detect the type of a field value
    
    Args:
        value: Value to detect the type of
        
    Returns:
        str: Type name ('string', 'number', 'boolean', 'array', 'object', or 'null')
    """
    if value is None:
        return 'null'
    elif isinstance(value, bool):
        return 'boolean'
    elif isinstance(value, (int, float)):
        return 'number'
    elif isinstance(value, str):
        return 'string'
    elif isinstance(value, list):
        return 'array'
    elif isinstance(value, dict):
        return 'object'
    else:
        return 'string'  # Default to string for unknown types

def generate_schema_from_json(data: Dict[str, Any], schema_name: str = 'Auto-generated Schema') -> Dict[str, Any]:
    """
    Generate a JSON schema from a JSON object
    
    Args:
        data: JSON data to generate a schema from
        schema_name: Name for the schema
        
    Returns:
        Dict[str, Any]: JSON schema
    """
    properties = {}
    required = []
    
    def process_object(obj: Dict[str, Any], parent_path: str = '') -> Dict[str, Any]:
        """
        Process a JSON object recursively to extract properties
        
        Args:
            obj: JSON object to process
            parent_path: Path to the current object
            
        Returns:
            Dict[str, Any]: Properties object
        """
        props = {}
        
        for key, value in obj.items():
            path = f"{parent_path}.{key}" if parent_path else key
            field_type = detect_field_type(value)
            
            if field_type == 'object':
                nested_props = process_object(value, path)
                props[key] = {
                    'type': field_type,
                    'properties': nested_props
                }
            elif field_type == 'array':
                if value and len(value) > 0:
                    # Try to determine array item type from the first element
                    first_item = value[0]
                    item_type = detect_field_type(first_item)
                    
                    if item_type == 'object':
                        item_props = process_object(first_item, f"{path}[]")
                        props[key] = {
                            'type': field_type,
                            'items': {
                                'type': item_type,
                                'properties': item_props
                            }
                        }
                    else:
                        props[key] = {
                            'type': field_type,
                            'items': {
                                'type': item_type
                            }
                        }
                else:
                    # Empty array, can't determine item type
                    props[key] = {
                        'type': field_type,
                        'items': {
                            'type': 'string'  # Default to string
                        }
                    }
            else:
                props[key] = {
                    'type': field_type
                }
        
        return props
    
    # Process the root object
    properties = process_object(data)
    
    # Generate the schema
    schema = {
        'title': schema_name,
        'type': 'object',
        'properties': properties,
    }
    
    return schema

def generate_schema_from_csv(file_path: str, schema_name: str = 'Auto-generated Schema') -> Dict[str, Any]:
    """
    Generate a JSON schema from a CSV file
    
    Args:
        file_path: Path to the CSV file
        schema_name: Name for the schema
        
    Returns:
        Dict[str, Any]: JSON schema
    """
    try:
        # Read the CSV file
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        if not rows:
            logger.warning(f"CSV file '{file_path}' is empty")
            return {
                'title': schema_name,
                'type': 'object',
                'properties': {}
            }
        
        # Extract field names and sample values
        field_names = reader.fieldnames or []
        sample_values = {}
        
        # Get sample values for each field
        for row in rows[:10]:  # Use up to 10 rows for sampling
            for field in field_names:
                if field not in sample_values:
                    sample_values[field] = []
                
                if field in row and row[field]:
                    sample_values[field].append(row[field])
        
        # Determine field types
        properties = {}
        
        for field in field_names:
            samples = sample_values.get(field, [])
            
            # Try to determine the best type
            is_number = all(s.replace('.', '', 1).isdigit() for s in samples if s)
            is_boolean = all(s.lower() in ['true', 'false', '0', '1', 'yes', 'no'] for s in samples if s)
            
            if is_boolean:
                field_type = 'boolean'
            elif is_number:
                field_type = 'number'
            else:
                field_type = 'string'
            
            properties[field] = {
                'type': field_type
            }
        
        # Generate the schema
        schema = {
            'title': schema_name,
            'type': 'object',
            'properties': properties
        }
        
        return schema
    except Exception as e:
        logger.error(f"Error generating schema from CSV file '{file_path}': {str(e)}")
        return {
            'title': schema_name,
            'type': 'object',
            'properties': {}
        }

def generate_schema_from_excel(file_path: str, schema_name: str = 'Auto-generated Schema') -> Dict[str, Any]:
    """
    Generate a JSON schema from an Excel file
    
    Args:
        file_path: Path to the Excel file
        schema_name: Name for the schema
        
    Returns:
        Dict[str, Any]: JSON schema
    """
    try:
        # Read the Excel file
        df = pd.read_excel(file_path)
        
        if df.empty:
            logger.warning(f"Excel file '{file_path}' is empty")
            return {
                'title': schema_name,
                'type': 'object',
                'properties': {}
            }
        
        # Extract field names and sample values
        field_names = df.columns.tolist()
        properties = {}
        
        for field in field_names:
            values = df[field].dropna().tolist()
            samples = values[:10]  # Use up to 10 values for sampling
            
            # Try to determine the best type
            if df[field].dtype == 'bool':
                field_type = 'boolean'
            elif df[field].dtype in ['int64', 'float64']:
                field_type = 'number'
            else:
                # Check if all values are boolean strings
                is_boolean = all(str(s).lower() in ['true', 'false', '0', '1', 'yes', 'no'] for s in samples if s)
                
                if is_boolean:
                    field_type = 'boolean'
                else:
                    field_type = 'string'
            
            properties[field] = {
                'type': field_type
            }
        
        # Generate the schema
        schema = {
            'title': schema_name,
            'type': 'object',
            'properties': properties
        }
        
        return schema
    except Exception as e:
        logger.error(f"Error generating schema from Excel file '{file_path}': {str(e)}")
        return {
            'title': schema_name,
            'type': 'object',
            'properties': {}
        }

def generate_schema_from_file(file_path: str, schema_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Generate a JSON schema from a file
    
    Args:
        file_path: Path to the file
        schema_name: Name for the schema (defaults to file name)
        
    Returns:
        Dict[str, Any]: JSON schema
    """
    # If schema name is not provided, use the file name without extension
    if schema_name is None:
        schema_name = os.path.splitext(os.path.basename(file_path))[0]
    
    # Get the file type
    file_type = get_file_type(file_path)
    
    # Generate schema based on file type
    if file_type == 'JSON':
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            return generate_schema_from_json(data, schema_name)
        except Exception as e:
            logger.error(f"Error reading JSON file '{file_path}': {str(e)}")
            return {
                'title': schema_name,
                'type': 'object',
                'properties': {}
            }
    elif file_type == 'CSV':
        return generate_schema_from_csv(file_path, schema_name)
    elif file_type == 'Excel':
        return generate_schema_from_excel(file_path, schema_name)
    else:
        logger.warning(f"File type '{file_type}' not supported for schema generation")
        return {
            'title': schema_name,
            'type': 'object',
            'properties': {}
        }

def merge_schemas(schemas: List[Dict[str, Any]], merged_name: str = 'Merged Schema') -> Dict[str, Any]:
    """
    Merge multiple schemas into a single schema
    
    Args:
        schemas: List of schemas to merge
        merged_name: Name for the merged schema
        
    Returns:
        Dict[str, Any]: Merged schema
    """
    if not schemas:
        return {
            'title': merged_name,
            'type': 'object',
            'properties': {}
        }
    
    # Start with the first schema
    merged = {
        'title': merged_name,
        'type': 'object',
        'properties': {}
    }
    
    # Add properties from all schemas
    for schema in schemas:
        if 'properties' in schema:
            properties = schema.get('properties', {})
            
            for prop_name, prop_def in properties.items():
                # If property already exists in merged schema, try to reconcile
                if prop_name in merged['properties']:
                    existing_prop = merged['properties'][prop_name]
                    
                    # If types are different, use a union type
                    if 'type' in existing_prop and 'type' in prop_def:
                        if existing_prop['type'] != prop_def['type']:
                            merged['properties'][prop_name]['type'] = [existing_prop['type'], prop_def['type']]
                    
                    # If both have nested properties, merge them recursively
                    if 'properties' in existing_prop and 'properties' in prop_def:
                        nested_merged = merge_schemas(
                            [{'properties': existing_prop['properties']}, {'properties': prop_def['properties']}],
                            f"{prop_name} Merged"
                        )
                        merged['properties'][prop_name]['properties'] = nested_merged['properties']
                else:
                    # Property doesn't exist yet, just add it
                    merged['properties'][prop_name] = prop_def
    
    return merged 