import logging
import os
import json
import subprocess
import time
import shutil
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app, Response
from typing import Dict, List, Any, Optional, Union, TypedDict, Tuple, cast, Literal
from datetime import datetime

from db import db, Schema, DatasetSchemaMapping, ExtractionProgress
from storage import create_storage, Storage
from ai import create_schema_generator, create_llm_extractor
from ai.extractor import DataExtractor
from constants import (
    STORAGE_TYPE, LOCAL_STORAGE_PATH, S3_BUCKET_NAME, AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY, AWS_REGION, USE_LOCAL_MODEL, LLM_PROVIDER,
    DEEPSEEK_API_KEY, OPENAI_API_KEY, ANTHROPIC_API_KEY, MODEL_CONFIGS,
    DEFAULT_LLM_PROVIDER, MAX_CHUNK_SIZE, DEFAULT_TEMPERATURE
)

logger = logging.getLogger(__name__)

extractors_bp = Blueprint('extractors', __name__, url_prefix='/api')

class FileResult(TypedDict, total=False):
    """Result of processing a single file"""
    filename: str
    status: Literal['success', 'error']
    output_file: Optional[str]
    message: Optional[str]

class ExtractorResponse(TypedDict, total=False):
    """Response for extraction endpoint"""
    success: bool
    error: Optional[str]
    dataset: Optional[str]
    output_directory: Optional[str]
    processed_files: Optional[int]
    results: Optional[List[FileResult]]

class StorageConfig(TypedDict, total=False):
    """Storage configuration options"""
    bucket_name: Optional[str]
    aws_access_key_id: Optional[str]
    aws_secret_access_key: Optional[str]
    region_name: Optional[str]
    storage_path: Optional[str]

def get_storage_config() -> StorageConfig:
    """Get storage configuration based on environment variables"""
    if STORAGE_TYPE == 's3':
        return {
            'bucket_name': S3_BUCKET_NAME,
            'aws_access_key_id': AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': AWS_SECRET_ACCESS_KEY,
            'region_name': AWS_REGION
        }
    else:
        return {
            'storage_path': LOCAL_STORAGE_PATH
        }

def get_extractor_config() -> Dict[str, Any]:
    """Get extractor configuration based on environment variables"""
    config: Dict[str, Any] = {
        'use_api': not USE_LOCAL_MODEL,
        'provider': LLM_PROVIDER
    }
    
    if not USE_LOCAL_MODEL:
        # Set API key based on provider
        if LLM_PROVIDER == 'deepseek':
            config['api_key'] = DEEPSEEK_API_KEY
        elif LLM_PROVIDER == 'openai':
            config['api_key'] = OPENAI_API_KEY
        elif LLM_PROVIDER == 'anthropic':
            config['api_key'] = ANTHROPIC_API_KEY
    
    # Get model and API URL from MODEL_CONFIGS
    mode = 'api' if not USE_LOCAL_MODEL else 'local'
    provider_config = MODEL_CONFIGS.get(LLM_PROVIDER, {}).get(mode, {})
    
    if 'model' in provider_config:
        config['model'] = provider_config['model']
    if 'api_url' in provider_config:
        config['api_url'] = provider_config['api_url']
    
    return config

@extractors_bp.route('/extract/<source>/<path:dataset_name>', methods=['POST'])
def extract_dataset(source: str, dataset_name: str) -> Tuple[Response, int]:
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
    schema_data: Optional[Dict[str, Any]] = None
    extraction_progress_record = None
    
    # Get schema from request JSON if provided
    if request.is_json:
        schema_data = request.get_json()
        logger.info(f"Using schema from request")
        
    try:
        from ai.task_manager import TaskManager
        task_manager = TaskManager.get_instance()
        
        logger.info(f"Starting extraction for dataset: {dataset_name} (source: {source})")
        
        # Check if an extraction is already running for this dataset
        if task_manager.is_extraction_running(source, dataset_name):
            logger.warning(f"Extraction already running for {source}/{dataset_name}")
            return jsonify({
                'warning': f'Extraction already in progress for {dataset_name}',
                'success': False
            }), 409
        
        # Get provider and model settings from request
        provider = request.args.get('provider', os.environ.get('LLM_PROVIDER', DEFAULT_LLM_PROVIDER))
        model = request.args.get('model')
        use_api_str = request.args.get('use_api', os.environ.get('USE_API', 'false'))
        use_api = use_api_str.lower() == 'true'
        
        temperature_str = request.args.get('temperature')
        temperature = float(temperature_str) if temperature_str else DEFAULT_TEMPERATURE
        
        # Get storage configuration and create storage instance
        storage_config = get_storage_config()
        storage = create_storage(STORAGE_TYPE, storage_config)
        
        # If schema not provided in request, get from database
        if not schema_data:
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
            schema_data = schema.schema
        
        # Get files in dataset using storage API
        files = storage.list_files(dataset_name)
        if not files:
            return jsonify({
                'error': f'No files found in dataset {dataset_name}'
            }), 404
            
        logger.info(f"Found {len(files)} files in dataset")
        
        # Create output directory
        output_dir = f"{current_app.config['DATA_DIR']}/extracted/{source}/{dataset_name}"
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")
        
        # Initialize extraction tracking in the database
        file_names = [file_info.get('name', '') for file_info in files]
        pdf_files = [f for f in file_names if f.lower().endswith('.pdf')]
        
        if not pdf_files:
            return jsonify({
                'error': f'No PDF files found in dataset {dataset_name}'
            }), 404
        
        # Create a new extraction progress record
        extraction_progress_record = ExtractionProgress(
            dataset_name=dataset_name,
            source=source,
            status='starting',
            total_files=len(pdf_files),
            processed_files=0,
            start_time=datetime.now()
        )
        # Set files list using setter method
        extraction_progress_record.set_files(pdf_files)
        
        session.add(extraction_progress_record)
        session.commit()
        extraction_progress_id = extraction_progress_record.id
        
        logger.info(f"Created extraction progress record with ID {extraction_progress_id}")
        
        # Create task info for the extraction
        task_info = {
            'extraction_progress_id': extraction_progress_id,
            'current_file_index': 0,
            'total_files': len(pdf_files),
            'files': pdf_files,
            'schema': schema_data,
            'output_dir': output_dir,
            'provider': provider,
            'model': model,
            'use_api': use_api,
            'temperature': temperature,
            'status': 'starting',
            'paused': False,
            'callback': lambda task_info: handle_dataset_extraction(task_info)
        }
        
        # Register the extraction with TaskManager
        task_manager.register_extraction(source, dataset_name, task_info)
        
        # Return success response with extraction_progress_id
        return jsonify({
            'success': True,
            'message': 'Extraction process has started',
            'dataset': dataset_name,
            'output_directory': output_dir,
            'total_files': len(pdf_files),
            'files': pdf_files,
            'extraction_progress_id': extraction_progress_id
        }), 202
        
    except Exception as e:
        logger.error(f"Error starting extraction process: {str(e)}", exc_info=True)
        
        # Mark extraction as failed in the database
        if extraction_progress_record:
            extraction_progress_record.status = 'failed'
            extraction_progress_record.message = f"Error starting extraction process: {str(e)}"
            extraction_progress_record.end_time = datetime.now()
            if extraction_progress_record.start_time:
                try:
                    extraction_progress_record.duration = (
                        datetime.now() - extraction_progress_record.start_time
                    ).total_seconds()
                except:
                    extraction_progress_record.duration = 0
            session.commit()
        
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)

def handle_dataset_extraction(task_info):
    """
    Process all files in a dataset extraction task
    
    This function is called by the TaskManager as a callback
    """
    try:
        logger.info(f"Starting dataset extraction task with {len(task_info['files'])} files")
        
        # Get variables from task_info
        extraction_progress_id = task_info['extraction_progress_id']
        source = task_info.get('source')
        dataset_name = task_info.get('dataset_name')
        files = task_info['files']
        schema = task_info['schema']
        output_dir = task_info['output_dir']
        provider = task_info['provider']
        model = task_info['model']
        use_api = task_info['use_api']
        temperature = task_info['temperature']
        
        # Update the extraction progress record status
        with db.get_session() as session:
            extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
            if extraction_progress_record:
                extraction_progress_record.status = 'in_progress'
                extraction_progress_record.message = 'Processing files'
                session.commit()
        
        # Process each file
        for i, filename in enumerate(files):
            # Skip files already processed if the task was resumed
            if i < task_info.get('current_file_index', 0):
                continue
                
            # Update current file index in task_info
            task_info['current_file_index'] = i
            
            # Check if task has been paused
            if task_info.get('paused', False):
                logger.info(f"Extraction task paused at file {i+1}/{len(files)}")
                # Update status in the database
                with db.get_session() as session:
                    extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
                    if extraction_progress_record:
                        extraction_progress_record.status = 'paused'
                        extraction_progress_record.message = 'Extraction paused by user'
                        session.commit()
                return
            
            # Update the extraction progress record
            with db.get_session() as session:
                extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
                if extraction_progress_record:
                    extraction_progress_record.current_file = filename
                    extraction_progress_record.file_progress = 0
                    session.commit()
            
            logger.info(f"Processing file {i+1}/{len(files)}: {filename}")
            
            # Process the file
            try:
                process_file(source, dataset_name, filename, output_dir, schema, None, provider, model, use_api, temperature)
                
                # Update processed files count
                with db.get_session() as session:
                    extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
                    if extraction_progress_record:
                        extraction_progress_record.processed_files += 1
                        session.commit()
                        
            except Exception as e:
                logger.error(f"Error processing file {filename}: {str(e)}", exc_info=True)
                # Continue with the next file despite errors
        
        # Complete the extraction in the database
        with db.get_session() as session:
            extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
            if extraction_progress_record:
                extraction_progress_record.status = 'completed'
                extraction_progress_record.message = f"Successfully processed {len(files)} files"
                extraction_progress_record.end_time = datetime.now()
                if extraction_progress_record.start_time:
                    try:
                        extraction_progress_record.duration = (
                            datetime.now() - extraction_progress_record.start_time
                        ).total_seconds()
                    except:
                        pass
                session.commit()
                
        logger.info(f"Completed dataset extraction task with {len(files)} files")
    except Exception as e:
        logger.error(f"Error in dataset extraction task: {str(e)}", exc_info=True)
        
        # Mark extraction as failed in the database
        with db.get_session() as session:
            extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
            if extraction_progress_record:
                extraction_progress_record.status = 'failed'
                extraction_progress_record.message = f"Error in extraction task: {str(e)}"
                extraction_progress_record.end_time = datetime.now()
                if extraction_progress_record.start_time:
                    try:
                        extraction_progress_record.duration = (
                            datetime.now() - extraction_progress_record.start_time
                        ).total_seconds()
                    except:
                        pass
                session.commit()
        
        raise

def process_file(source, dataset_name, filename, output_dir, schema=None, existing_schema_id=None, provider=None, model=None, use_api=None, temperature=None):
    """Process a single file from a dataset by converting it to markdown and extracting data"""
    try:
        from ai.task_manager import TaskManager
        
        # Get TaskManager instance
        task_manager = TaskManager.get_instance()
        
        session = db.get_session()
        
        # Create a new extraction progress record
        extraction_progress_record = ExtractionProgress(
            dataset_name=dataset_name,
            source=source,
            status='starting',
            message='Initializing extraction process',
            total_files=1,
            processed_files=0,
            start_time=datetime.now()
        )
        # Set JSON fields using proper setter methods
        extraction_progress_record.set_files([filename])
        extraction_progress_record.set_merged_data({})
        extraction_progress_record.set_merge_reasoning_history([])
        
        session.add(extraction_progress_record)
        session.commit()
        
        # Get the progress record ID
        extraction_progress_id = extraction_progress_record.id
        
        logger.info(f"Created extraction progress record with ID: {extraction_progress_id}")
        
        # Get storage configuration and create storage instance
        storage_config = get_storage_config()
        storage = create_storage(STORAGE_TYPE, storage_config)
        
        # Check if file exists in storage
        file_obj = storage.get_file(dataset_name, filename)
        if not file_obj:
            raise FileNotFoundError(f"File {filename} not found in dataset {dataset_name}")
        
        # Create output directory if it doesn't exist
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Get the base filename without extension
        base_filename = Path(filename).stem
        
        # Define output paths
        md_path = output_dir / f"{base_filename}.md"
        json_path = output_dir / f"{base_filename}.json"
        
        # Update progress
        extraction_progress_record.status = 'in_progress'
        extraction_progress_record.message = 'Converting PDF to Markdown'
        extraction_progress_record.current_file = filename
        session.commit()
        
        # Save the file from storage to a temporary location for processing
        temp_file_path = Path(current_app.config['DATA_DIR']) / 'temp' / filename
        os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)
        
        with open(temp_file_path, 'wb') as f:
            file_obj.seek(0)
            shutil.copyfileobj(file_obj, f)
        
        # 1. Convert PDF to Markdown if needed
        if filename.lower().endswith('.pdf') and not md_path.exists():
            logger.info(f"Converting PDF to Markdown: {temp_file_path} -> {md_path}")
            extraction_progress_record.file_progress = 0.3
            session.commit()
            
            convert_pdf_to_markdown(str(temp_file_path), str(md_path))
        else:
            # If not a PDF or markdown already exists, copy the file
            extraction_progress_record.file_progress = 0.3
            session.commit()
            
            if not md_path.exists():
                # If the file is already in markdown format, just copy it
                shutil.copy(temp_file_path, md_path)
                logger.info(f"Copied file to output directory: {temp_file_path} -> {md_path}")
        
        # Clean up the temporary file
        os.remove(temp_file_path)
        
        # Update progress
        extraction_progress_record.status = 'in_progress'
        extraction_progress_record.message = 'Preparing schema for extraction'
        session.commit()
        
        # Get or create schema
        if schema:
            # Use provided schema
            logger.info(f"Using provided schema for extraction")
        elif existing_schema_id:
            # Fetch schema from database
            logger.info(f"Fetching schema with ID {existing_schema_id} from database")
            schema_record = session.query(Schema).filter_by(id=existing_schema_id).first()
            if not schema_record:
                raise ValueError(f"Schema with ID {existing_schema_id} not found")
            schema = schema_record.json_schema
        else:
            # No schema provided or specified, use default
            raise ValueError("No schema provided or specified")
        
        # Check for empty schema
        if not schema:
            raise ValueError("Empty schema provided")
        
        # Set up the LLM extractor
        extraction_progress_record.status = 'in_progress'
        extraction_progress_record.message = 'Initializing LLM extractor'
        extraction_progress_record.file_progress = 0.5
        session.commit()
        
        logger.info(f"Setting up LLM extractor with provider={provider}")
        
        # Configure the extractor
        extractor_config = {
            'provider': provider,
            'use_api': use_api,
            'model': model,
            'temperature': temperature
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
        
        # Update progress before starting extraction
        extraction_progress_record.file_progress = 0.6
        extraction_progress_record.message = 'Starting data extraction'
        session.commit()
        
        logger.info(f"Starting data extraction from Markdown using schema")
        
        # Define the task info for TaskManager
        extraction_key = (source, dataset_name)
        task_info = {
            'extraction_progress_id': extraction_progress_id,
            'current_chunk': 0,
            'total_chunks': 0,  # Will be updated during extraction
            'status': 'in_progress',
            'paused': False,
            'callback': lambda task_info: handle_extraction_task(task_info, md_path, schema, extractor, source, dataset_name, filename, extraction_progress_id, json_path)
        }
        
        # Register the extraction task
        task_manager.register_extraction(source, dataset_name, task_info)
        
        # Return the extraction progress record
        return {
            'filename': filename,
            'status': 'started',
            'extraction_progress_id': extraction_progress_id,
            'message': 'Extraction process started'
        }
            
    except Exception as e:
        logger.error(f"Error processing file {filename}: {str(e)}", exc_info=True)
        
        # Update extraction progress with error
        if 'extraction_progress_record' in locals() and extraction_progress_record:
            extraction_progress_record.status = 'failed'
            extraction_progress_record.message = f"Error: {str(e)}"
            extraction_progress_record.end_time = datetime.now()
            if extraction_progress_record.start_time:
                extraction_progress_record.duration = (
                    datetime.now() - extraction_progress_record.start_time
                ).total_seconds()
            session.commit()
        
        return {
            'filename': filename,
            'status': 'error',
            'message': str(e)
        }
    finally:
        db.close_session(session)

def handle_extraction_task(task_info, md_path, schema, extractor, source, dataset_name, filename, extraction_progress_id, json_path):
    """
    Handle the actual extraction task from a TaskManager callback
    """
    try:
        logger.info(f"Starting extraction task for {filename}")
        
        # Extract data from the Markdown file
        extracted_data = extract_data_from_markdown(md_path, schema, extractor, source, dataset_name, filename, extraction_progress_id)
        
        # Save the extracted data to JSON
        with open(json_path, 'w') as f:
            json.dump(extracted_data, f, indent=2)
        
        logger.info(f"Successfully completed extraction task for {filename}")
        return extracted_data
    except Exception as e:
        logger.error(f"Error in extraction task for {filename}: {str(e)}", exc_info=True)
        
        # Update extraction progress with error
        with db.get_session() as session:
            extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
            if extraction_progress_record:
                extraction_progress_record.status = 'failed'
                extraction_progress_record.message = f"Error: {str(e)}"
                extraction_progress_record.end_time = datetime.now()
                if extraction_progress_record.start_time:
                    extraction_progress_record.duration = (
                        datetime.now() - extraction_progress_record.start_time
                    ).total_seconds()
                session.commit()
        
        raise

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


def split_content_into_chunks(content: str, max_chunk_size: int = MAX_CHUNK_SIZE) -> List[str]:
    """Split markdown content into smaller chunks while preserving structure."""
    chunks: List[str] = []
    current_chunk: List[str] = []
    current_size: int = 0
    
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
    
    # If accumulated_data is empty, return chunk_data directly
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
                        # New period, add it
                        merged[key].append(period_data)
            else:
                # For other lists, append new items
                merged[key].extend([x for x in value if x not in merged[key]])
    
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

def extract_data_from_markdown(markdown_file: Path, schema: Dict[str, Any], extractor: DataExtractor, 
                               source: str, dataset_name: str, filename: str, extraction_progress_id: int) -> Dict[str, Any]:
    """
    Extract structured data from a markdown file using the provided extractor
    
    Args:
        markdown_file: Path to the markdown file
        schema: JSON schema defining the structure of the data
        extractor: DataExtractor instance to use for extraction
        source: Source of the dataset (for progress tracking)
        dataset_name: Name of the dataset (for progress tracking)
        filename: Name of the file being processed (for progress tracking)
        extraction_progress_id: ID of the extraction progress record
        
    Returns:
        Extracted data as a dictionary
    """
    from ai.task_manager import TaskManager
    
    # Get the task manager instance
    task_manager = TaskManager.get_instance()
    
    # Read the markdown file
    with open(markdown_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Progress tracking enabled if source, dataset_name, and filename are provided
    track_progress = source and dataset_name and filename
    
    if track_progress:
        logger.info(f"Progress tracking enabled for {source}_{dataset_name}, file: {filename}")
    
    # Split the content into chunks
    chunks = split_content_into_chunks(content)
    total_chunks = len(chunks)
    
    logger.info(f"Split content into {total_chunks} chunks")
    
    # Update total chunks for this file if tracking progress
    if track_progress:
        with db.get_session() as session:
            extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
            extraction_progress_record.current_file_chunks = total_chunks
            extraction_progress_record.total_chunks += total_chunks
            extraction_progress_record.current_file_chunk = 0
            session.commit()
    
    # Process each chunk
    all_chunk_results = []
    merged_data = {}
    merge_reasoning_history = []
    
    # Register this extraction with the task manager
    extraction_key = (source, dataset_name)
    task_info = {
        'extraction_progress_id': extraction_progress_id,
        'current_chunk': 0,
        'total_chunks': total_chunks,
        'status': 'in_progress',
        'paused': False
    }
    
    # If we're not starting a new extraction but continuing one,
    # retrieve the previous task state from TaskManager
    existing_task = task_manager.get_extraction_info(source, dataset_name)
    if existing_task:
        # Update with existing task info
        last_processed_chunk = existing_task.get('current_chunk', 0)
        task_info['current_chunk'] = last_processed_chunk
        logger.info(f"Resuming extraction from chunk {last_processed_chunk}/{total_chunks}")
    else:
        # Register as new extraction
        task_manager.register_extraction(source, dataset_name, task_info)
    
    for i, chunk in enumerate(chunks):
        # Skip chunks that were already processed if resuming
        if existing_task and i < task_info['current_chunk']:
            logger.info(f"Skipping already processed chunk {i+1}/{total_chunks}")
            continue
        
        logger.info(f"Processing chunk {i+1}/{total_chunks}")
        
        # Update task info with current chunk
        task_info['current_chunk'] = i
        
        # Check if extraction has been paused
        if task_info.get('paused', False):
            logger.info(f"Extraction paused at chunk {i+1}/{total_chunks}")
            break
        
        # Update progress if tracking enabled
        if track_progress:
            with db.get_session() as session:
                extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
                extraction_progress_record.file_progress = (i + 1) / total_chunks
                extraction_progress_record.current_file_chunk = i + 1
                extraction_progress_record.status = 'in_progress'
                session.commit()
        
        # Create a prompt for this chunk
        prompt = create_extraction_prompt_with_context(chunk, schema, i, len(chunks))
        
        # Extract data from the chunk
        chunk_data = extractor.extract_data_with_context(prompt, schema)
        
        # Add the chunk index to the result
        all_chunk_results.append({
            'chunk_index': i,
            'data': chunk_data
        })
        
        # If we have processed data from multiple chunks, do an intermediate merge
        # and report the current state for progress tracking
        if track_progress and i > 0 and i % 2 == 0:
            logger.info(f"Performing intermediate merge after chunk {i+1}/{total_chunks}")
            # Create a prompt for merging the current results with reasoning
            intermediate_merge_prompt = create_intermediate_merge_prompt(all_chunk_results[:i+1], schema)
            
            # Get an intermediate merged result with reasoning
            intermediate_result = extractor.merge_results_with_reasoning(intermediate_merge_prompt, schema)
            
            # Extract the merged data and reasoning
            intermediate_merged_data = intermediate_result.get('merged_data', {})
            merge_reasoning = intermediate_result.get('reasoning', {})
            
            # Add reasoning to history with timestamp and chunk info
            timestamp = int(time.time())
            reasoning_entry = {
                "timestamp": timestamp,
                "chunk_index": i,
                "total_chunks": total_chunks,
                "reasoning": merge_reasoning
            }
            merge_reasoning_history.append(reasoning_entry)
            
            # Update the merged data for progress tracking
            logger.debug(f"Updating merged data for: {source}, {dataset_name}")
            with db.get_session() as session:
                extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
                extraction_progress_record.set_merged_data_with_reasoning(intermediate_merged_data, reasoning_entry)
                session.commit()
    
    # Check if extraction was completed or paused
    if task_info.get('paused', False):
        logger.info(f"Extraction paused - saving current state")
        with db.get_session() as session:
            extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
            extraction_progress_record.status = 'paused'
            extraction_progress_record.message = 'Extraction paused by user'
            session.commit()
        return merged_data
    
    # If there's only one chunk, return its data
    if len(all_chunk_results) == 1:
        logger.info("Only one chunk processed, returning its data directly")
        result_data = all_chunk_results[0]['data'].get('data', {})
        
        # Add a simple reasoning entry for single chunk
        timestamp = int(time.time())
        reasoning_entry = {
            "timestamp": timestamp,
            "chunk_index": 0,
            "total_chunks": 1,
            "reasoning": {"single_chunk": "Only one chunk was processed, so no merging was necessary."},
            "is_final": True
        }
        merge_reasoning_history.append(reasoning_entry)
        
        # Update the merged data for progress tracking
        if track_progress:
            logger.debug(f"Updating final merged data for: {source}, {dataset_name}")
            with db.get_session() as session:
                extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
                extraction_progress_record.set_merged_data_with_reasoning(result_data, reasoning_entry)
                extraction_progress_record.status = 'completed'
                extraction_progress_record.end_time = datetime.now()
                extraction_progress_record.duration = (
                    datetime.now() - extraction_progress_record.start_time
                ).total_seconds()
                session.commit()
        
        # Cleanup task from task manager
        task_manager.cancel_extraction(source, dataset_name)
        
        return result_data
    
    # Create a prompt for merging all the results
    logger.info(f"Creating final merge prompt for {len(all_chunk_results)} chunks")
    final_merge_prompt = create_intermediate_merge_prompt(all_chunk_results, schema)
    
    # Get the merged result from the LLM with reasoning
    logger.info("Getting final merged result from LLM")
    final_result = extractor.merge_results_with_reasoning(final_merge_prompt, schema)
    
    # Extract the merged data and reasoning
    merged_data = final_result.get('merged_data', {})
    final_reasoning = final_result.get('reasoning', {})
    
    # Add final reasoning to history
    timestamp = int(time.time())
    final_reasoning_entry = {
        "timestamp": timestamp,
        "chunk_index": len(all_chunk_results) - 1,
        "total_chunks": total_chunks,
        "reasoning": final_reasoning,
        "is_final": True
    }
    merge_reasoning_history.append(final_reasoning_entry)
    
    # Update the merged data for progress tracking
    if track_progress:
        logger.debug(f"Updating final merged data for: {source}, {dataset_name}")
        with db.get_session() as session:
            extraction_progress_record = session.query(ExtractionProgress).get(extraction_progress_id)
            extraction_progress_record.set_merged_data_with_reasoning(merged_data, final_reasoning_entry)
            extraction_progress_record.status = 'completed'
            extraction_progress_record.end_time = datetime.now()
            extraction_progress_record.duration = (
                datetime.now() - extraction_progress_record.start_time
            ).total_seconds()
            session.commit()
    
    # Cleanup task from task manager
    task_manager.cancel_extraction(source, dataset_name)
    
    return merged_data

def create_extraction_prompt_with_context(content: str, schema: Dict[str, Any], chunk_index: int, total_chunks: int) -> str:
    """
    Create a prompt for extracting data with contextual information
    
    Args:
        content: The content to extract data from
        schema: JSON schema defining the structure of the data
        chunk_index: Index of the current chunk (0-based)
        total_chunks: Total number of chunks
        
    Returns:
        A prompt for the LLM to extract data with context
    """
    # Convert schema to a string representation
    schema_str = json.dumps(schema, indent=2)
    
    # Create the prompt
    prompt = f"""
You are an expert at extracting structured data from documents. I have a chunk of a document (chunk {chunk_index + 1} of {total_chunks}), and I need you to extract data from it in order
to populate a set of fields defined in a schema.

The data should all be relevant to the data in this schema:
{schema_str}

Here is the chunk of the document:
{content}

Please extract the data from this chunk and provide it in a JSON object. For each field you extract, also provide metadata about the extraction:

1. page_number: The page number where this information was found (if available)
2. prominence: How prominent this information is in the document (e.g., "header", "title", "main text", "footnote")
3. format: The format of the information (e.g., "table", "paragraph", "list", "heading")
4. confidence: Your confidence in the relevance of the extraction to filling in the schema {schema_str} (0.0 to 1.0)

Return your response in this format:
{{
  "data": {{
    // The extracted data according to the schema
  }},
  "metadata": {{
    // For each field in the data, provide metadata
    "field_name": {{
      "page_number": 1,
      "prominence": "header",
      "format": "table",
      "confidence": 0.53
    }}
  }}
}}

Again, the data you output should all be relevant to the data in this schema:
{schema_str}

Return only the JSON object, with no additional text or explanation.
"""
    
    return prompt

def create_merge_prompt(chunk_results: List[Dict[str, Any]], schema: Dict[str, Any]) -> str:
    """
    Create a prompt for the LLM to merge multiple chunk results
    
    Args:
        chunk_results: List of chunk results with their indices and metadata
        schema: JSON schema defining the structure of the data
        
    Returns:
        A prompt for the LLM to merge the results
    """
    # Convert schema to a string representation
    schema_str = json.dumps(schema, indent=2)
    
    # Create a string representation of all chunk results
    chunk_results_str = ""
    for result in chunk_results:
        chunk_index = result['chunk_index']
        chunk_data = result['data']
        
        # Format the data and metadata
        data_str = json.dumps(chunk_data.get('data', {}), indent=2)
        metadata_str = json.dumps(chunk_data.get('metadata', {}), indent=2)
        
        chunk_results_str += f"Chunk {chunk_index}:\n"
        chunk_results_str += f"Data:\n{data_str}\n\n"
        chunk_results_str += f"Metadata:\n{metadata_str}\n\n"
    
    # Create the prompt
    prompt = f"""
You are an expert at extracting structured data from documents. I have extracted data from different chunks of a document, and I need you to merge these results into a single, coherent JSON object.

The data should conform to this schema:
{schema_str}

Here are the extracted data from different chunks of the document, along with metadata about each field:
{chunk_results_str}

Please merge these results into a single JSON object that best represents the document. Consider the following:

1. For basic fields (like title, date, company name), prefer values from earlier chunks (especially the first chunk) as these typically appear at the beginning of documents.

2. For lists of items (like time periods), combine all unique items from all chunks.

3. For nested objects, merge them intelligently, taking the most complete information.

4. If there are conflicting values, use your judgment to determine which is most likely correct based on context.

5. For financial data, ensure consistency across time periods.

6. Pay special attention to the metadata for each field:
   - Prefer values with higher confidence levels
   - Consider the prominence of the information in the document
   - Take into account the format of the information (e.g., tables are often more reliable for structured data)
   - Use page numbers to understand the document structure

7. For fields that appear in multiple chunks with different values, use the metadata to determine which value is most likely correct.

Return only the merged JSON object, with no additional text or explanation.
"""
    
    return prompt

def create_intermediate_merge_prompt(chunk_results: List[Dict[str, Any]], schema: Dict[str, Any]) -> str:
    """
    Create a prompt specifically for intermediate merges with reasoning
    
    Args:
        chunk_results: List of chunk results with their indices and metadata
        schema: JSON schema defining the structure of the data
        
    Returns:
        A prompt for the LLM to merge the results with reasoning
    """
    # Convert schema to a string representation
    schema_str = json.dumps(schema, indent=2)
    
    # Create a string representation of all chunk results
    chunk_results_str = ""
    for result in chunk_results:
        chunk_index = result['chunk_index']
        chunk_data = result['data']
        
        # Format the data and metadata
        data_str = json.dumps(chunk_data.get('data', {}), indent=2)
        metadata_str = json.dumps(chunk_data.get('metadata', {}), indent=2)
        
        chunk_results_str += f"Chunk {chunk_index}:\n"
        chunk_results_str += f"Data:\n{data_str}\n\n"
        chunk_results_str += f"Metadata:\n{metadata_str}\n\n"
    
    # Create the prompt with request for reasoning
    prompt = f"""
You are an expert at extracting structured data from documents. I have extracted data from different chunks of a document, and I need you to merge these results into a single, coherent JSON object.

The data should conform to this schema:
{schema_str}

Here are the extracted data from different chunks of the document, along with metadata about each field:
{chunk_results_str}

Please merge these results into a single JSON object that best represents the document, following these guidelines:

1. For basic fields (like title, date, company name), prefer values from earlier chunks (especially the first chunk) as these typically appear at the beginning of documents.
2. For lists of items (like time periods), combine all unique items from all chunks.
3. For nested objects, merge them intelligently, taking the most complete information.
4. If there are conflicting values, use your judgment to determine which is most likely correct based on context.
5. For financial data, ensure consistency across time periods.
6. Pay special attention to the metadata for each field (confidence, prominence, format, page numbers).

Provide your response in this format:
{{
  "merged_data": {{
    // The merged data according to the schema
  }},
  "reasoning": {{
    // For each field where you made a significant merging decision, explain your reasoning
    "field_name": "Explanation of how you merged this field and why"
  }}
}}

The "reasoning" section should include your thought process for any non-trivial merge decisions, especially where there were conflicts or where you had to make judgments about which values to prefer.
"""
    
    return prompt

@extractors_bp.route('/clear-extraction-state/<source>/<path:dataset_name>', methods=['POST'])
def clear_extraction_state(source: str, dataset_name: str) -> Tuple[Response, int]:
    """
    Clear the extraction state for a dataset
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        
    Returns:
        JSON response with success status
    """
    try:
        logger.info(f"Clearing extraction state for dataset: {dataset_name} (source: {source})")
        extraction_progress_record = db.get_session().query(ExtractionProgress).filter_by(
            dataset_name=dataset_name,
            source=source
        ).first()
        if extraction_progress_record:
            extraction_progress_record.status = 'cleared'
            db.get_session().commit()
        return jsonify({
            'success': True,
            'message': f"Extraction state cleared for {dataset_name}"
        }), 200
    except Exception as e:
        logger.error(f"Error clearing extraction state: {str(e)}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500 

@extractors_bp.route('/extract/status/<source>/<path:dataset_name>', methods=['GET'])
def get_extraction_status(source: str, dataset_name: str) -> Tuple[Response, int]:
    """Get the status of an extraction job."""
    extraction_progress_record = db.get_session().query(ExtractionProgress).filter_by(
        dataset_name=dataset_name,
        source=source
    ).first()
    if extraction_progress_record:
        return jsonify(extraction_progress_record.to_dict()), 200
    return jsonify({'exists': False, 'message': 'No extraction status found'}), 200


@extractors_bp.route('/extract/state/<source>/<path:dataset_name>', methods=['GET'])
def get_extraction_state(source: str, dataset_name: str) -> Tuple[Response, int]:
    """Get the current state of an extraction job."""
    extraction_progress_record = db.get_session().query(ExtractionProgress).filter_by(
        dataset_name=dataset_name,
        source=source
    ).first()
    if extraction_progress_record:
        return jsonify(extraction_progress_record.to_dict()), 200
    return jsonify({'exists': False, 'message': 'No extraction state found'}), 200 