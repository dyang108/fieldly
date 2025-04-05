import logging
import os
import json
import subprocess
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app, Response
from typing import Dict, List, Any, Optional, Union, TypedDict, Tuple, cast, Literal

from db import db, Schema, DatasetSchemaMapping
from storage import create_storage, Storage
from ai import create_schema_generator, create_llm_extractor
from ai.extractor import DataExtractor
from utils import extraction_progress
from constants import (
    STORAGE_TYPE, LOCAL_STORAGE_PATH, S3_BUCKET_NAME, AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY, AWS_REGION, USE_LOCAL_MODEL, LLM_PROVIDER,
    DEEPSEEK_API_KEY, OPENAI_API_KEY, ANTHROPIC_API_KEY, MODEL_CONFIGS,
    DEFAULT_LLM_PROVIDER, MAX_CHUNK_SIZE
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
    
    # Get schema from request JSON if provided
    if request.is_json:
        schema_data = request.get_json()
        logger.info(f"Using schema from request")
        
    try:
        logger.info(f"Starting extraction for dataset: {dataset_name} (source: {source})")
        
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
        
        # Initialize extraction tracking
        file_names = [file_info.get('name', '') for file_info in files]
        pdf_files = [f for f in file_names if f.lower().endswith('.pdf')]
        extraction_progress.start_extraction(source, dataset_name, pdf_files)
        
        # Process each file
        results: List[FileResult] = []
        merged_data = {}
        
        for file_info in files:
            filename = file_info.get('name', '')
            if not filename.lower().endswith('.pdf'):
                logger.info(f"Skipping non-PDF file: {filename}")
                continue
                
            # Update current file in progress
            extraction_progress.update_file_progress(source, dataset_name, filename, 0.2)
            
            # Process the file
            result = process_file(storage, dataset_name, output_dir, filename, schema_data, source)
            results.append(result)
            
            # Mark file as completed
            extraction_progress.file_completed(source, dataset_name, filename)
            
        # Complete the extraction
        extraction_progress.complete_extraction(
            source, 
            dataset_name, 
            success=True, 
            message=f"Successfully processed {len(results)} files"
        )
            
        return jsonify({
            'success': True,
            'dataset': dataset_name,
            'output_directory': output_dir,
            'processed_files': len(results),
            'results': results
        }), 200
        
    except Exception as e:
        logger.error(f"Error in extraction process: {str(e)}", exc_info=True)
        
        # Mark extraction as failed
        extraction_progress.complete_extraction(
            source, 
            dataset_name, 
            success=False, 
            message=f"Error in extraction process: {str(e)}"
        )
        
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)


def process_file(storage: Storage, dataset_name: str, output_dir: str, 
                filename: str, schema: Dict[str, Any], source: str) -> FileResult:
    """Process a single file through the extraction pipeline"""
    try:
        logger.info(f"Starting extraction pipeline for file: {filename}")
        logger.info(f"Using schema with {len(schema)} top-level fields")
        
        # Get local path if using local storage
        if isinstance(storage.config.get('storage_path'), str):
            # For local storage
            base_path = Path(cast(str, storage.config['storage_path']))
            pdf_path = base_path / dataset_name / filename
            
            # Use cached directory for intermediate files
            cached_dir = base_path / "cached"
            md_path = cached_dir / f"{dataset_name}-md" / f"{filename}.md"
            json_path = base_path / output_dir / f"{filename}.json"
            
            # Create markdown directory if it doesn't exist
            md_dir = cached_dir / f"{dataset_name}-md"
            md_dir.mkdir(exist_ok=True, parents=True)
            
            # 1. Convert PDF to Markdown if not already done
            extraction_progress.update_file_progress(source, dataset_name, filename, 0.2)
            if not md_path.exists():
                logger.info(f"Converting PDF to Markdown: {pdf_path}")
                convert_pdf_to_markdown(str(pdf_path), str(md_path))
                logger.info("PDF conversion completed")
            else:
                logger.info("Using existing Markdown file")
            
            # Get AI configuration from environment variables
            provider = os.environ.get('LLM_PROVIDER', DEFAULT_LLM_PROVIDER)
            use_api = os.environ.get('USE_API', 'false').lower() == 'true'
            
            # Configure the extractor
            extraction_progress.update_file_progress(source, dataset_name, filename, 0.4)
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
            
            # 2. Extract data from Markdown based on schema
            extraction_progress.update_file_progress(source, dataset_name, filename, 0.6)
            logger.info(f"Starting data extraction from Markdown using schema")
            extracted_data = extract_data_from_markdown(md_path, schema, extractor, source, dataset_name, filename)
            
            # Send update with the extracted data
            extraction_progress.update_merged_data(source, dataset_name, extracted_data)
            
            # 3. Save extracted data as JSON
            extraction_progress.update_file_progress(source, dataset_name, filename, 0.9)
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

def extract_data_from_markdown(markdown_file: Path, schema: Dict[str, Any], extractor: DataExtractor, source: str = "", dataset_name: str = "", filename: str = "") -> Dict[str, Any]:
    """
    Extract structured data from a markdown file using the provided extractor
    
    Args:
        markdown_file: Path to the markdown file
        schema: JSON schema defining the structure of the data
        extractor: DataExtractor instance to use for extraction
        source: Source of the dataset (for progress tracking)
        dataset_name: Name of the dataset (for progress tracking)
        filename: Name of the file being processed (for progress tracking)
        
    Returns:
        Extracted data as a dictionary
    """
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
    
    # Process each chunk
    all_chunk_results = []
    merged_data = {}
    
    for i, chunk in enumerate(chunks):
        logger.info(f"Processing chunk {i+1}/{total_chunks}")
        
        # Update progress if tracking enabled
        if track_progress:
            chunk_progress = 0.6 + (0.3 * (i / total_chunks))
            logger.debug(f"Updating file progress: {source}, {dataset_name}, {filename}, {chunk_progress:.2f}")
            extraction_progress.update_file_progress(source, dataset_name, filename, chunk_progress)
        
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
            # Create a prompt for merging the current results
            intermediate_merge_prompt = create_merge_prompt(all_chunk_results[:i+1], schema)
            
            # Get an intermediate merged result
            intermediate_merged_data = extractor.merge_results(intermediate_merge_prompt, schema)
            
            # Update the merged data for progress tracking
            logger.debug(f"Updating merged data for: {source}, {dataset_name}")
            extraction_progress.update_merged_data(source, dataset_name, intermediate_merged_data)
    
    # If there's only one chunk, return its data
    if len(all_chunk_results) == 1:
        logger.info("Only one chunk processed, returning its data directly")
        result_data = all_chunk_results[0]['data'].get('data', {})
        
        # Update the merged data for progress tracking
        if track_progress:
            logger.debug(f"Updating final merged data for: {source}, {dataset_name}")
            extraction_progress.update_merged_data(source, dataset_name, result_data)
            
        return result_data
    
    # Create a prompt for merging all the results
    logger.info(f"Creating final merge prompt for {len(all_chunk_results)} chunks")
    merge_prompt = create_merge_prompt(all_chunk_results, schema)
    
    # Get the merged result from the LLM
    logger.info("Getting final merged result from LLM")
    merged_data = extractor.merge_results(merge_prompt, schema)
    
    # Update the merged data for progress tracking
    if track_progress:
        logger.debug(f"Updating final merged data for: {source}, {dataset_name}")
        extraction_progress.update_merged_data(source, dataset_name, merged_data)
    
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