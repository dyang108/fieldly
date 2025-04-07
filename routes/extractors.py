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
import threading
from db import db, Schema, DatasetSchemaMapping, ExtractionProgress
from storage import create_storage, Storage
from ai import create_schema_generator, create_llm_extractor
from ai.extractor import DataExtractor
from utils import extraction_progress
from constants import (
    STORAGE_TYPE, LOCAL_STORAGE_PATH, S3_BUCKET_NAME, AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY, AWS_REGION, USE_LOCAL_MODEL, LLM_PROVIDER,
    DEEPSEEK_API_KEY, OPENAI_API_KEY, ANTHROPIC_API_KEY, MODEL_CONFIGS,
    DEFAULT_LLM_PROVIDER, MAX_CHUNK_SIZE, DEFAULT_TEMPERATURE, DATA_DIR
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
    4. Queue the extraction for processing by the batch processor
    """
    session = db.get_session()
    schema_data: Optional[Dict[str, Any]] = None
    
    # Get schema from request JSON if provided
    if request.is_json:
        schema_data = request.get_json()
        logger.info(f"Using schema from request")
        
    try:
        logger.info(f"Starting extraction for dataset: {dataset_name} (source: {source})")
        
        # First check if an extraction is active
        is_active = False
        extraction_progress_id = None
        resumed = False
        
        # Check if there's an active extraction in the database
        extraction_progress_id = extraction_progress.resume_extraction(source, dataset_name)
        if extraction_progress_id:
            is_active = True
            resumed = True
            logger.info(f"Resumed extraction from database for {source}/{dataset_name}")
        
        # If extraction is active, get its state and return
        if is_active and extraction_progress_id:
            # Get the current extraction state
            extraction_state = extraction_progress.get_extraction_state(source, dataset_name)
            
            return jsonify({
                'success': True,
                'message': 'Continuing with existing extraction process',
                'dataset': dataset_name,
                'extraction_progress_id': extraction_progress_id,
                'resumed': resumed
            }), 200
        
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
        output_dir = f"{DATA_DIR}/extracted/{source}/{dataset_name}"
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")
        
        # Initialize extraction tracking in the database
        file_names = [file_info.get('name', '') for file_info in files]
        pdf_files = [f for f in file_names if f.lower().endswith('.pdf')]
        
        if not pdf_files:
            return jsonify({
                'error': f'No PDF files found in dataset {dataset_name}'
            }), 404
        
        # Create a new extraction progress record and get its ID
        extraction_progress_id = extraction_progress.start_extraction(source, dataset_name, pdf_files)
        
        # Store the schema
        with db.get_session() as session:
            extraction_record = session.query(ExtractionProgress).get(extraction_progress_id)
            if extraction_record and schema_data:
                extraction_record.schema = json.dumps(schema_data, indent=2) if isinstance(schema_data, dict) else schema_data
                extraction_record.status = 'scheduled'  # Set to scheduled for batch processing
                extraction_record.message = 'Extraction scheduled for processing'
                extraction_record.provider = provider
                extraction_record.model = model
                extraction_record.use_api = use_api
                extraction_record.temperature = temperature
                session.commit()
        
        logger.info(f"Created extraction progress record with ID {extraction_progress_id}")
        
        # Return success response with extraction_progress_id
        return jsonify({
            'success': True,
            'message': 'Extraction process has been scheduled',
            'dataset': dataset_name,
            'output_directory': output_dir,
            'total_files': len(pdf_files),
            'files': pdf_files,
            'extraction_progress_id': extraction_progress_id
        }), 202
        
    except Exception as e:
        logger.error(f"Error starting extraction process: {str(e)}", exc_info=True)
        
        # Mark extraction as failed
        extraction_progress.complete_extraction(source, dataset_name, False, f"Error starting extraction process: {str(e)}")
        
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)

def handle_dataset_extraction(extraction_progress_id, source, dataset_name, files, schema, output_dir, provider, model, use_api, temperature):
    """
    Process all files in a dataset extraction task
    
    This function is called as a background thread to process files
    """
    try:
        logger.info(f"Starting dataset extraction task with {len(files)} files")
        
        # Update the extraction progress record status
        extraction_progress.update_extraction_progress(
            source, 
            dataset_name, 
            {
                'status': 'in_progress',
                'message': 'Processing files'
            }
        )
        
        # Process each file
        for i, filename in enumerate(files):
            # Check if extraction has been paused or cancelled
            current_status = extraction_progress.get_extraction_status(source, dataset_name)
            
            if not current_status or current_status == 'cancelled':
                logger.info(f"Extraction cancelled for {source}/{dataset_name}")
                return
            
            if current_status == 'paused':
                logger.info(f"Extraction paused for {source}/{dataset_name} at file {i+1}/{len(files)}")
                return
            
            # Update the current file information
            extraction_progress.update_extraction_progress(
                source, 
                dataset_name, 
                {
                    'current_file': filename,
                    'current_file_index': i,
                    'file_progress': 0
                }
            )
            
            logger.info(f"Processing file {i+1}/{len(files)}: {filename}")
            
            # Process the file
            try:
                process_file(filename, source, dataset_name, get_extractor_config())
                
                # Update processed files count
                extraction_progress.update_extraction_progress(
                    source, 
                    dataset_name, 
                    {
                        'processed_files': i + 1
                    }
                )
                
            except Exception as e:
                logger.error(f"Error processing file {filename}: {str(e)}", exc_info=True)
                # Continue with the next file despite errors
        
        # Complete the extraction
        extraction_progress.complete_extraction(
            source, 
            dataset_name, 
            True, 
            f"Successfully processed {len(files)} files"
        )
        
        logger.info(f"Completed dataset extraction task with {len(files)} files")
    except Exception as e:
        logger.error(f"Error in dataset extraction task: {str(e)}", exc_info=True)
        
        # Mark extraction as failed
        extraction_progress.complete_extraction(
            source, 
            dataset_name, 
            False, 
            f"Error in extraction task: {str(e)}"
        )
        
        raise

def process_file(file_path: str, source: str, dataset_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single file with the given configuration"""
    try:
        # Check if there's an active extraction for this dataset
        active_extraction = db.session.query(ExtractionProgress).filter(
            ExtractionProgress.source == source,
            ExtractionProgress.dataset_name == dataset_name,
            ExtractionProgress.status == 'in_progress',
            ExtractionProgress.end_time.is_(None)
        ).first()
        
        if active_extraction:
            extraction_progress_id = active_extraction.id
        else:
            # Create a new extraction progress record
            extraction_progress = ExtractionProgress(
                source=source,
                dataset_name=dataset_name,
                status='in_progress',
                current_file=file_path,
                total_files=1,
                processed_files=0
            )
            db.session.add(extraction_progress)
            db.session.commit()
            extraction_progress_id = extraction_progress.id
        
        # Create the extractor
        try:
            extractor = create_llm_extractor(config)
        except ValueError as e:
            if "API key is required" in str(e):
                # If API key is missing, retry with use_api=False
                config['use_api'] = False
                extractor = create_llm_extractor(config)
            else:
                raise
        
        # Process the file
        result = extractor.extract(file_path)
        
        # Update extraction progress
        extraction_progress = db.session.query(ExtractionProgress).get(extraction_progress_id)
        if extraction_progress:
            extraction_progress.processed_files += 1
            extraction_progress.status = 'completed'
            extraction_progress.end_time = datetime.utcnow()
            db.session.commit()
        
        return result
    except Exception as e:
        logger.exception(f"Error processing file {file_path}: {e}")
        # Update extraction progress to failed
        if 'extraction_progress_id' in locals():
            extraction_progress = db.session.query(ExtractionProgress).get(extraction_progress_id)
            if extraction_progress:
                extraction_progress.status = 'failed'
                extraction_progress.message = str(e)
                extraction_progress.end_time = datetime.utcnow()
                db.session.commit()
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
    # Read the markdown file
    with open(markdown_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split the content into chunks
    chunks = split_content_into_chunks(content)
    total_chunks = len(chunks)
    
    logger.info(f"Split content into {total_chunks} chunks")
    
    # Update extraction progress with initial chunk information
    extraction_progress.update_extraction_progress(
        source, 
        dataset_name, 
        {
            'status': 'in_progress',
            'message': 'Processing files',
            'total_chunks': total_chunks,
            'current_chunk': 0,
            'merged_data': None,
            'merge_reasoning_history': []
        }
    )
    
    # Process each chunk
    all_chunk_results = []
    merged_data = {}
    merge_reasoning_history = []
    current_chunk = 0
    
    # Check if we need to resume from a specific chunk
    extraction_state = extraction_progress.get_extraction_state(source, dataset_name)
    if extraction_state and extraction_state.get('status') == 'paused':
        current_chunk = extraction_state.get('current_chunk', 0)
        logger.info(f"Resuming extraction from chunk {current_chunk}/{total_chunks}")
    
    for i, chunk in enumerate(chunks):
        # Skip chunks that were already processed if resuming
        if i < current_chunk:
            logger.info(f"Skipping already processed chunk {i+1}/{total_chunks}")
            continue
        
        logger.info(f"Processing chunk {i+1}/{total_chunks}")
        
        # Update current chunk in the database
        extraction_state = extraction_progress.get_extraction_state(source, dataset_name)
        if extraction_state:
            # Check if extraction has been paused or cancelled
            if extraction_state.get('status') == 'paused':
                logger.info(f"Extraction paused at chunk {i+1}/{total_chunks}")
                break
            elif extraction_state.get('status') == 'cancelled':
                logger.info(f"Extraction cancelled")
                break
        
        # Update extraction progress with current chunk
        extraction_progress.update_extraction_progress(
            source, 
            dataset_name, 
            {
                'current_chunk': i + 1,
                'message': f'Processing chunk {i+1}/{total_chunks}'
            }
        )
        
        # Create a prompt for this chunk
        prompt = create_extraction_prompt_with_context(chunk, schema, i, len(chunks))
        
        # Extract data from the chunk
        chunk_data = extractor.extract_data_with_context(prompt, schema)
        
        # Add the chunk index to the result
        all_chunk_results.append({
            'chunk_index': i,
            'data': chunk_data
        })

        logger.info(f'Chunk {i+1}/{total_chunks} processed, data: {chunk_data}')
        
        # If we have processed data from multiple chunks, do an intermediate merge
        # and report the current state for progress tracking
        if i > 0 and i % 2 == 0:
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
            extraction_progress.update_extraction_progress(
                source,
                dataset_name,
                {
                    'merged_data': intermediate_merged_data,
                    'merge_reasoning_history': merge_reasoning_history
                }
            )
    
    # Check if extraction was completed or paused
    extraction_state = extraction_progress.get_extraction_state(source, dataset_name)
    if extraction_state and extraction_state.get('status') == 'paused':
        logger.info(f"Extraction paused - saving current state")
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
        logger.debug(f"Updating final merged data for: {source}, {dataset_name}")
        extraction_progress.update_extraction_progress(
            source,
            dataset_name,
            {
                'merged_data': result_data,
                'merge_reasoning_history': merge_reasoning_history
            }
        )
        
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
    
    # Save final result to database
    extraction_progress.update_extraction_progress(
        source, 
        dataset_name, 
        {
            'merged_data': merged_data,
            'merge_reasoning_history': merge_reasoning_history,
            'file_progress': 1.0
        }
    )
    
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

@extractors_bp.route('/delete-running-extraction/<source>/<path:dataset_name>', methods=['POST'])
def delete_running_extraction_endpoint(source: str, dataset_name: str) -> Tuple[Response, int]:
    """
    Delete a running extraction for a dataset
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        
    Returns:
        JSON response with success status
    """
    try:
        logger.info(f"Deleting running extraction for dataset: {dataset_name} (source: {source})")
        success = extraction_progress.delete_running_extraction(source, dataset_name)
        
        if success:
            return jsonify({
                'success': True,
                'message': f"Running extraction deleted for {dataset_name}"
            }), 200
        else:
            return jsonify({
                'success': False,
                'message': f"No running extraction found for {dataset_name}"
            }), 404
    except Exception as e:
        logger.error(f"Error deleting running extraction: {str(e)}", exc_info=True)
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