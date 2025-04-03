import logging
import os
import json
import subprocess
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app
from typing import Dict, List, Any, Optional

from db import db, Schema, DatasetSchemaMapping
from storage import create_storage
from ai import create_schema_generator, create_llm_extractor
from ai.extractor import DataExtractor
from constants import DEFAULT_LLM_PROVIDER

logger = logging.getLogger(__name__)

extractors_bp = Blueprint('extractors', __name__, url_prefix='/api')

@extractors_bp.route('/extract/<source>/<path:dataset_name>', methods=['POST'])
def extract_dataset(source: str, dataset_name: str):
    """
    Extract structured data from a dataset using its associated schema
    
    1. Find the dataset directory
    2. Get the associated schema from the database
    3. Create output directory for extracted data
    4. For each PDF file:
       a. Convert to Markdown
       b. Extract structured data based on schema
       c. Save as JSON
    """
    session = db.get_session()
    try:
        logger.info(f"Starting extraction for dataset: {dataset_name} (source: {source})")
        
        # Get storage configuration
        storage_type = current_app.config.get('STORAGE_TYPE', 'local')
        storage_config = {}
        
        if storage_type == 's3':
            storage_config = {
                'bucket_name': current_app.config.get('S3_BUCKET_NAME'),
                'aws_access_key_id': current_app.config.get('AWS_ACCESS_KEY_ID'),
                'aws_secret_access_key': current_app.config.get('AWS_SECRET_ACCESS_KEY'),
                'region_name': current_app.config.get('AWS_REGION')
            }
        else:
            storage_config = {
                'storage_path': current_app.config.get('LOCAL_STORAGE_PATH', '.data')
            }
        
        # Create storage instance
        storage = create_storage(storage_type, storage_config)
        
        # Get dataset mapping to find schema
        mapping = session.query(DatasetSchemaMapping).filter_by(
            dataset_name=dataset_name,
            source=source
        ).first()
        
        if not mapping or not mapping.schema_id:
            return jsonify({
                'error': f'No schema associated with dataset {dataset_name}'
            }), 400
        
        # Get schema
        schema = session.query(Schema).get(mapping.schema_id)
        if not schema:
            return jsonify({
                'error': f'Schema with ID {mapping.schema_id} not found'
            }), 404
            
        logger.info(f"Using schema: {schema.name} (ID: {schema.id})")
        
        # Get files in dataset
        files = storage.list_files(dataset_name)
        if not files:
            return jsonify({
                'error': f'No files found in dataset {dataset_name}'
            }), 404
            
        logger.info(f"Found {len(files)} files in dataset")
        
        # Create output directory
        output_dir = f"{dataset_name}-extracted"
        if not storage.dataset_exists(output_dir):
            storage.create_dataset(output_dir)
            logger.info(f"Created output directory: {output_dir}")
        
        # Process each file
        results = []
        for file_info in files:
            filename = file_info.get('name')
            if not filename.lower().endswith('.pdf'):
                logger.info(f"Skipping non-PDF file: {filename}")
                continue
                
            result = process_file(storage, dataset_name, output_dir, filename, schema.schema)
            results.append(result)
            
        return jsonify({
            'success': True,
            'dataset': dataset_name,
            'output_directory': output_dir,
            'processed_files': len(results),
            'results': results
        })
        
    except Exception as e:
        logger.error(f"Error in extraction process: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)


def process_file(storage, dataset_name: str, output_dir: str, filename: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single file through the extraction pipeline"""
    try:
        logger.info(f"Starting extraction pipeline for file: {filename}")
        logger.info(f"Using schema with {len(schema)} top-level fields")
        
        # Get local path if using local storage
        if isinstance(storage.config.get('storage_path'), str):
            # For local storage
            base_path = Path(storage.config['storage_path'])
            pdf_path = base_path / dataset_name / filename
            md_path = base_path / f"{dataset_name}-md" / f"{filename}.md"
            json_path = base_path / output_dir / f"{filename}.json"
            
            # Create markdown directory if it doesn't exist
            md_dir = base_path / f"{dataset_name}-md"
            md_dir.mkdir(exist_ok=True, parents=True)
            
            # 1. Convert PDF to Markdown if not already done
            if not md_path.exists():
                logger.info(f"Converting PDF to Markdown: {pdf_path}")
                convert_pdf_to_markdown(str(pdf_path), str(md_path))
                logger.info("PDF conversion completed")
            else:
                logger.info("Using existing Markdown file")
            
            # 2. Extract data from Markdown based on schema
            logger.info(f"Starting data extraction from Markdown using schema")
            extracted_data = extract_data_from_markdown(str(md_path), schema)
            
            # 3. Save extracted data as JSON
            logger.info(f"Saving extracted data to: {json_path}")
            with open(json_path, 'w') as f:
                json.dump(extracted_data, f, indent=2)
                
            logger.info(f"Successfully completed extraction pipeline for {filename}")
            
            return {
                'filename': filename,
                'status': 'success',
                'output_file': str(json_path)
            }
            
    except Exception as e:
        logger.error(f"Error processing file {filename}: {str(e)}", exc_info=True)
        return {
            'filename': filename,
            'status': 'error',
            'message': str(e)
        }


def convert_pdf_to_markdown(pdf_path: str, md_path: str) -> None:
    """Convert a PDF file to Markdown using pymupdf4llm"""
    try:
        # Import pymupdf4llm for PDF to Markdown conversion
        import pymupdf4llm
        
        # Convert PDF to Markdown using pymupdf4llm
        markdown_content = pymupdf4llm.to_markdown(pdf_path)
        
        # Save the markdown content to file
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
            
        logger.info(f"Converted PDF to Markdown: {md_path}")
        
    except Exception as e:
        logger.error(f"Error converting PDF to Markdown: {str(e)}", exc_info=True)
        raise


def split_content_into_chunks(content: str, max_chunk_size: int = 4000) -> List[str]:
    """Split markdown content into smaller chunks while preserving structure."""
    chunks = []
    current_chunk = []
    current_size = 0
    
    # Split by paragraphs
    paragraphs = content.split('\n\n')
    
    for para in paragraphs:
        para_size = len(para)
        
        # If adding this paragraph would exceed max size, start a new chunk
        if current_size + para_size > max_chunk_size and current_chunk:
            chunks.append('\n\n'.join(current_chunk))
            current_chunk = []
            current_size = 0
        
        current_chunk.append(para)
        current_size += para_size
    
    # Add the last chunk if it exists
    if current_chunk:
        chunks.append('\n\n'.join(current_chunk))
    
    return chunks

def merge_chunk_data(accumulated_data: Dict[str, Any], chunk_data: Dict[str, Any]) -> Dict[str, Any]:
    """Merge data from a chunk into accumulated data, taking the first non-null value for each field."""
    if not accumulated_data:
        return chunk_data
    
    merged = accumulated_data.copy()
    
    # Handle basic fields
    for key, value in chunk_data.items():
        if key not in merged or merged[key] is None:
            merged[key] = value
        elif isinstance(value, dict) and isinstance(merged[key], dict):
            # Recursively merge dictionaries
            for subkey, subvalue in value.items():
                if subkey not in merged[key] or merged[key][subkey] is None:
                    merged[key][subkey] = subvalue
        elif isinstance(value, list) and isinstance(merged[key], list):
            # For lists, we need to handle special cases
            if key == "timePeriods":
                # For time periods, merge by period
                for period_data in value:
                    period = period_data.get("period")
                    if not period:
                        continue
                    
                    # Check if period already exists
                    existing_period = next(
                        (p for p in merged[key] if p.get("period") == period),
                        None
                    )
                    
                    if existing_period:
                        # Merge metrics
                        for metric, metric_value in period_data.get("metrics", {}).items():
                            if metric_value is not None and metric not in existing_period["metrics"]:
                                existing_period["metrics"][metric] = metric_value
                    else:
                        merged[key].append(period_data)
            else:
                # For other lists, just append new items
                for item in value:
                    if item not in merged[key]:
                        merged[key].append(item)
    
    return merged

def print_accumulated_data(data: Dict[str, Any], schema: Dict[str, Any], indent: int = 0) -> None:
    """
    Print accumulated data as formatted JSON, filtering to only include fields defined in the schema.
    
    Args:
        data: The accumulated data to print
        schema: The schema defining the allowed fields
        indent: Current indentation level (used for recursive calls)
    """
    def filter_by_schema(data: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively filter data to only include fields defined in the schema."""
        if not isinstance(data, dict) or not isinstance(schema, dict):
            return data
            
        filtered = {}
        for key, value in data.items():
            if key in schema:
                if isinstance(value, dict) and isinstance(schema[key], dict):
                    # Recursively filter nested dictionaries
                    filtered[key] = filter_by_schema(value, schema[key])
                elif isinstance(value, list) and isinstance(schema[key], dict) and "items" in schema[key]:
                    # Handle arrays of objects
                    filtered[key] = [
                        filter_by_schema(item, schema[key]["items"])
                        for item in value
                    ]
                else:
                    # Keep basic fields as is
                    filtered[key] = value
        return filtered
    
    # Filter the data according to the schema
    filtered_data = filter_by_schema(data, schema)
    
    # Filter out None values for cleaner output
    filtered_data = {k: v for k, v in filtered_data.items() if v is not None}
    
    # Print formatted JSON
    logger.info(json.dumps(filtered_data, indent=2))

def extract_data_from_markdown(md_path: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract structured data from a markdown file according to a schema
    
    Args:
        md_path: Path to the markdown file
        schema: JSON schema defining the structure of the data to extract
        
    Returns:
        Extracted data as a dictionary matching the schema
    """
    # Read the markdown file
    with open(md_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split content into chunks
    chunks = split_content_into_chunks(content)
    logger.info(f"Split content into {len(chunks)} chunks")
    
    # Get AI configuration from app config
    use_api = current_app.config.get('USE_API', 'false').lower() == 'true'
    provider = current_app.config.get('LLM_PROVIDER', DEFAULT_LLM_PROVIDER)
    
    # Configure the extractor
    extractor_config = {
        'use_api': use_api,
        'provider': provider
    }
    
    if use_api:
        # API keys for different providers
        if provider == 'deepseek':
            extractor_config['api_key'] = current_app.config.get('DEEPSEEK_API_KEY')
            extractor_config['api_url'] = current_app.config.get('DEEPSEEK_API_URL')
        elif provider == 'openai':
            extractor_config['api_key'] = current_app.config.get('OPENAI_API_KEY')
            extractor_config['api_url'] = current_app.config.get('OPENAI_API_URL')
        elif provider == 'anthropic':
            extractor_config['api_key'] = current_app.config.get('ANTHROPIC_API_KEY')
            extractor_config['api_url'] = current_app.config.get('ANTHROPIC_API_URL')
    else:
        # Local model configuration
        extractor_config['model'] = current_app.config.get(f'{provider.upper()}_LOCAL_MODEL')
        extractor_config['api_url'] = current_app.config.get(f'{provider.upper()}_LOCAL_API_URL')
    
    # Initialize the extractor using the factory function
    extractor = create_llm_extractor(**extractor_config)
    
    # Process each chunk and accumulate data
    accumulated_data = {}
    for i, chunk in enumerate(chunks, 1):
        logger.info(f"Processing chunk {i}/{len(chunks)}")
        
        # Extract data from the chunk
        chunk_data = extractor.extract_data(chunk, schema)
        
        # Merge the chunk data into accumulated data
        accumulated_data = merge_chunk_data(accumulated_data, chunk_data)
        
        # Log the accumulated data
        logger.info(f"Accumulated data after chunk {i}:")
        print_accumulated_data(accumulated_data, schema)
    
    return accumulated_data 