import logging
import time
import threading
import os
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import io
import tempfile

from db import db, ExtractionProgress
from utils import extraction_progress
from storage import create_storage
from ai import create_llm_extractor
from constants import (
    STORAGE_TYPE, LOCAL_STORAGE_PATH, S3_BUCKET_NAME, AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY, AWS_REGION, USE_LOCAL_MODEL, LLM_PROVIDER,
    DEEPSEEK_API_KEY, OPENAI_API_KEY, ANTHROPIC_API_KEY, MODEL_CONFIGS,
    DEFAULT_LLM_PROVIDER, MAX_CHUNK_SIZE, DEFAULT_TEMPERATURE, DATA_DIR
)

logger = logging.getLogger(__name__)

# Import pymupdf4llm for PDF processing
try:
    import pymupdf4llm
    PDF_SUPPORT = True
except ImportError:
    logger.warning("pymupdf4llm not installed. PDF processing will not be available.")
    PDF_SUPPORT = False

def get_storage_config() -> Dict[str, Any]:
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

def convert_pdf_to_markdown(file_path: str, source: str, dataset_name: str) -> str:
    """Convert a PDF file to markdown format and save it to a temporary directory"""
    try:
        print(f"[PDF Processing] Starting conversion of {file_path} to markdown")
        
        # Get storage configuration and create storage instance
        storage_config = get_storage_config()
        storage = create_storage(STORAGE_TYPE, storage_config)
        
        # Check if file exists in storage
        print(f"[PDF Processing] Retrieving file from storage: {file_path}")
        file_obj = storage.get_file(dataset_name, file_path)
        if not file_obj:
            print(f"[PDF Processing] ERROR: File {file_path} not found in dataset {dataset_name}")
            raise FileNotFoundError(f"File {file_path} not found in dataset {dataset_name}")
        
        # Read the file content from storage
        file_obj.seek(0)
        file_content = file_obj.read()
        file_size = len(file_content)
        print(f"[PDF Processing] Retrieved file content, size: {file_size} bytes")
        
        # Create a temporary file for the PDF
        print(f"[PDF Processing] Creating temporary file for PDF processing")
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name
            print(f"[PDF Processing] Temporary file created at: {temp_file_path}")
        
        try:
            # Convert PDF to markdown using pymupdf4llm
            print(f"[PDF Processing] Converting PDF to markdown using pymupdf4llm")
            markdown_content = pymupdf4llm.to_markdown(temp_file_path)
            
            print(f"[PDF Processing] Conversion complete, markdown size: {len(markdown_content)} characters")
            if not markdown_content.strip():
                print(f"[PDF Processing] WARNING: No text content extracted from PDF")
                markdown_content = "No text content could be extracted from this PDF file."
            
            # Save markdown content to a file in the .data directory
            markdown_dir = Path(f"{DATA_DIR}/cached/{source}/{dataset_name}-md")
            os.makedirs(markdown_dir, exist_ok=True)
            
            # Create a filename based on the original PDF filename
            markdown_filename = os.path.splitext(os.path.basename(file_path))[0] + '.md'
            markdown_file_path = markdown_dir / markdown_filename
            
            print(f"[PDF Processing] Saving markdown content to {markdown_file_path}")
            
            # Save the markdown content
            with open(markdown_file_path, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            print(f"[PDF Processing] Markdown saved successfully")
            logger.info(f"Saved markdown content to {markdown_file_path}")
                
            return markdown_content
        finally:
            # Clean up the temporary PDF file
            print(f"[PDF Processing] Cleaning up temporary file")
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
                print(f"[PDF Processing] Temporary file removed")
                
    except Exception as e:
        print(f"[PDF Processing] ERROR: {str(e)}")
        logger.exception(f"Error converting PDF to markdown for {file_path}: {e}")
        raise

def process_file(file_path: str, source: str, dataset_name: str, config: Dict[str, Any], markdown_content: Optional[str] = None) -> Dict[str, Any]:
    """Process a single file with the given configuration"""
    session = None
    try:
        print(f"\n[LLM Extraction] Starting extraction for file: {file_path}")
        session = db.get_session()
        
        # Check if there's an active extraction for this dataset
        print(f"[LLM Extraction] Looking for active extraction record for {source}/{dataset_name}")
        active_extraction = session.query(ExtractionProgress).filter(
            ExtractionProgress.source == source,
            ExtractionProgress.dataset_name == dataset_name,
            ExtractionProgress.status.in_(['in_progress', 'scheduled', 'paused'])
        ).order_by(ExtractionProgress.updated_at.desc()).first()
        
        if active_extraction:
            extraction_progress_id = active_extraction.id
            schema = active_extraction.get_schema()
            
            # If the status is not 'in_progress', update it
            if active_extraction.status != 'in_progress':
                active_extraction.status = 'in_progress'
                session.commit()
                
            print(f"[LLM Extraction] Using existing extraction progress record {extraction_progress_id} for {source}/{dataset_name}")
            logger.info(f"Using existing extraction progress record {extraction_progress_id} for {source}/{dataset_name}")
        else:
            # Create a new extraction progress record
            print(f"[LLM Extraction] Creating new extraction progress record for {source}/{dataset_name}")
            extraction_progress_record = ExtractionProgress(
                source=source,
                dataset_name=dataset_name,
                status='in_progress',
                current_file=file_path,
                total_files=1,
                processed_files=0
            )
            session.add(extraction_progress_record)
            session.commit()
            extraction_progress_id = extraction_progress_record.id
            schema = None
            print(f"[LLM Extraction] Created extraction progress record with ID {extraction_progress_id}")
        
        # Create the extractor
        print(f"[LLM Extraction] Creating LLM extractor with config: {config}")
        try:
            extractor = create_llm_extractor(config)
            print(f"[LLM Extraction] Extractor created: provider={extractor.provider}, model={extractor.model}, use_api={extractor.use_api}")
        except ValueError as e:
            if "API key is required" in str(e):
                # If API key is missing, retry with use_api=False
                print(f"[LLM Extraction] API key missing, falling back to local model")
                config['use_api'] = False
                extractor = create_llm_extractor(config)
                print(f"[LLM Extraction] Extractor created: provider={extractor.provider}, model={extractor.model}, use_api={extractor.use_api}")
            else:
                raise
        
        # Skip storage steps if markdown content is provided
        if markdown_content is not None:
            print(f"[LLM Extraction] Using provided markdown content, skipping storage retrieval")
            content = markdown_content
            print(f"[LLM Extraction] Content size: {len(content)} characters")
        else:
            # Get storage configuration and create storage instance
            print(f"[LLM Extraction] Retrieving file from storage: {file_path}")
            storage_config = get_storage_config()
            storage = create_storage(STORAGE_TYPE, storage_config)
            
            # Check if file exists in storage
            file_obj = storage.get_file(dataset_name, file_path)
            if not file_obj:
                print(f"[LLM Extraction] ERROR: File {file_path} not found in dataset {dataset_name}")
                raise FileNotFoundError(f"File {file_path} not found in dataset {dataset_name}")
            
            # Read the file content from storage
            file_obj.seek(0)
            file_content = file_obj.read()
            print(f"[LLM Extraction] Retrieved file content, size: {len(file_content)} bytes")
            
            # Process the file based on its extension
            if file_path.lower().endswith('.pdf'):
                if not PDF_SUPPORT:
                    print(f"[LLM Extraction] ERROR: PDF support not available, pymupdf4llm not installed")
                    raise ImportError("pymupdf4llm is not installed. Cannot process PDF files.")
                
                # Process PDF file using pymupdf4llm
                # Save the file content to a temporary file
                print(f"[LLM Extraction] Processing PDF file")
                temp_file = Path(f"/tmp/{os.path.basename(file_path)}")
                with open(temp_file, 'wb') as f:
                    f.write(file_content)
                
                # Convert PDF to markdown using pymupdf4llm
                print(f"[LLM Extraction] Converting PDF to markdown using pymupdf4llm")
                content = pymupdf4llm.to_markdown(str(temp_file))
                
                # Clean up the temporary file
                if temp_file.exists():
                    temp_file.unlink()
                
                if not content.strip():
                    print(f"[LLM Extraction] WARNING: No text content extracted from PDF")
                    content = "No text content could be extracted from this PDF file."
                    
                print(f"[LLM Extraction] PDF converted to markdown, size: {len(content)} characters")
            else:
                # For non-PDF files, try to decode as text
                print(f"[LLM Extraction] Processing non-PDF file, attempting to decode text")
                try:
                    content = file_content.decode('utf-8')
                    print(f"[LLM Extraction] Successfully decoded with UTF-8")
                except UnicodeDecodeError:
                    # If UTF-8 fails, try other encodings
                    print(f"[LLM Extraction] UTF-8 decoding failed, trying alternative encodings")
                    for encoding in ['latin-1', 'cp1252', 'iso-8859-1']:
                        try:
                            content = file_content.decode(encoding)
                            print(f"[LLM Extraction] Successfully decoded with {encoding}")
                            break
                        except UnicodeDecodeError:
                            continue
                    else:
                        # If all decodings fail, use a placeholder
                        print(f"[LLM Extraction] All decoding attempts failed, using placeholder message")
                        content = f"Binary file content could not be decoded as text. File: {file_path}"
        
        # Split content into chunks if it's too large
        if len(content) > MAX_CHUNK_SIZE:
            print(f"[LLM Extraction] Content exceeds MAX_CHUNK_SIZE ({MAX_CHUNK_SIZE}), splitting into chunks")
            chunks = []
            for i in range(0, len(content), MAX_CHUNK_SIZE):
                chunks.append(content[i:i + MAX_CHUNK_SIZE])
            print(f"[LLM Extraction] Split content into {len(chunks)} chunks of max size {MAX_CHUNK_SIZE}")
        else:
            print(f"[LLM Extraction] Content size ({len(content)}) is within MAX_CHUNK_SIZE, using single chunk")
            chunks = [content]
        
        # Update extraction progress with chunk information
        print(f"[LLM Extraction] Updating extraction progress with chunk information")
        extraction_progress.update_extraction_progress(
            source, 
            dataset_name, 
            {
                'current_file': file_path,
                'total_chunks': len(chunks),
                'current_chunk': 0,
                'file_progress': 0,
                'message': f'Processing file {os.path.basename(file_path)}'
            }
        )
        
        # Create a prompt for extraction
        print(f"[LLM Extraction] Creating extraction prompt template")
        prompt_template = """
You are an expert at extracting structured data from documents. I have a document, and I need you to extract data from it in order
to populate a set of fields defined in a schema.

The data should all be relevant to the data in this schema:
{schema}

Here is the document:
{content}

Please extract the data from this document and provide it in a JSON object. For each field you extract, also provide metadata about the extraction:

1. page_number: The page number where this information was found (if available)
2. prominence: How prominent this information is in the document (e.g., "header", "title", "main text", "footnote")
3. format: The format of the information (e.g., "table", "paragraph", "list", "heading")
4. confidence: Your confidence in the relevance of the extraction to filling in the schema (0.0 to 1.0).

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

Remember, all data extracted should conform to the schema:
{schema}

Return only the JSON object, with no additional text or explanation.
"""
        
        # If schema is None, provide a default schema to avoid errors
        if schema is None:
            # Create a default schema that allows any data structure
            print(f"[LLM Extraction] No schema provided, creating default schema")
            default_schema = {
                "type": "object",
                "properties": {},
                "additionalProperties": True
            }
            schema_to_use = default_schema
            schema_text = "No schema provided"
        else:
            schema_to_use = schema
            schema_text = str(schema)
            print(f"[LLM Extraction] Using schema: {schema_text[:100]}{'...' if len(schema_text) > 100 else ''}")
        
        chunk_results = []
        
        # Process each chunk
        for i, chunk in enumerate(chunks):
            # Update extraction progress with chunk information
            print(f"\n[LLM Extraction] Processing chunk {i+1}/{len(chunks)}, size: {len(chunk)} characters")
            
            # Update the current chunk being processed
            extraction_progress.update_extraction_progress(
                source, 
                dataset_name, 
                {
                    'current_file': file_path,
                    'current_chunk': i + 1,
                    'message': f'Processing chunk {i+1}/{len(chunks)} of {os.path.basename(file_path)}'
                }
            )
            
            logger.info(f"Processing chunk {i+1}/{len(chunks)} of {file_path}")
            
            # Create prompt for this chunk
            print(f"[LLM Extraction] Creating prompt for chunk {i+1}")
            prompt = prompt_template.format(schema=schema_text, content=chunk)
            print(f"[LLM Extraction] Prompt size: {len(prompt)} characters")
            
            # Process the chunk
            print(f"[LLM Extraction] Sending prompt to extractor for processing")
            result = extractor.extract_data_with_context(prompt, schema_to_use)
            print(f"[LLM Extraction] Received extraction result: {json.dumps(result, indent=2)}...")
            
            chunk_results.append(result)
            
            # Log chunk result and update progress
            print(f"[LLM Extraction] Updating extraction progress with chunk result")
            extraction_progress.update_extraction_progress(
                source, 
                dataset_name, 
                {
                    'current_file': file_path,
                    'current_chunk': i + 1,
                    'message': f'Processed chunk {i+1}/{len(chunks)} of {os.path.basename(file_path)}'
                }
            )
        
        # Merge results if there are multiple chunks
        if len(chunk_results) > 1:
            print(f"\n[LLM Extraction] Merging {len(chunk_results)} chunk results for {file_path}")
            
            # Update progress to indicate merging
            extraction_progress.update_extraction_progress(
                source, 
                dataset_name, 
                {
                    'current_file': file_path,
                    'message': f'Merging results from {len(chunk_results)} chunks'
                }
            )
            
            # Create merge prompt with previous results
            print(f"[LLM Extraction] Calling extractor.merge_results to merge chunks")
            merge_explanation = extractor.merge_results(chunk_results)
            print(f"[LLM Extraction] Merge explanation: {merge_explanation}")
            
            # Store the final result with merge explanation
            final_result = chunk_results[-1]  # Use the last merged result
            final_result['merge_explanation'] = merge_explanation
            
            # Update progress with merge explanation
            print(f"[LLM Extraction] Updating extraction progress with final merged result")
            extraction_progress.update_extraction_progress(
                source, 
                dataset_name, 
                {
                    'current_file': file_path,
                    'message': f'Completed processing {os.path.basename(file_path)}',
                    'merge_explanation': merge_explanation
                }
            )
        else:
            print(f"[LLM Extraction] Single chunk processed, no merging required")
            final_result = chunk_results[0]
        
        # Update extraction progress
        print(f"[LLM Extraction] Updating extraction progress record to indicate completion")
        with db.get_session() as update_session:
            extraction_progress_record = update_session.query(ExtractionProgress).get(extraction_progress_id)
            if extraction_progress_record:
                extraction_progress_record.processed_files += 1
                update_session.commit()
                print(f"[LLM Extraction] Extraction progress record updated")
        
        print(f"[LLM Extraction] Processing completed for {file_path}")
        return final_result
    except Exception as e:
        print(f"[LLM Extraction] ERROR processing file {file_path}: {str(e)}")
        logger.exception(f"Error processing file {file_path}: {e}")
        # Update extraction progress to failed
        if session and 'extraction_progress_id' in locals():
            print(f"[LLM Extraction] Updating extraction progress to failed state")
            extraction_progress.update_extraction_progress(
                source, 
                dataset_name, 
                {
                    'status': 'failed',
                    'message': f'Error processing file {file_path}: {str(e)}'
                }
            )
        raise
    finally:
        if session:
            db.close_session(session)

def handle_dataset_extraction(extraction_progress_id, source, dataset_name, files, schema, output_dir, provider=None, model=None, use_api=None, temperature=None):
    """
    Process all files in a dataset extraction task
    
    This function is called to process files in the background
    """
    try:
        print(f"\n[Extraction Task] Starting dataset extraction task with {len(files)} files")
        print(f"[Extraction Task] Source: {source}, Dataset: {dataset_name}")
        logger.info(f"Starting dataset extraction task with {len(files)} files")
        
        # Update the extraction progress record status
        print(f"[Extraction Task] Updating extraction progress status to 'in_progress'")
        extraction_progress.update_extraction_progress(
            source, 
            dataset_name, 
            {
                'status': 'in_progress',
                'message': 'Processing files',
                'total_chunks': 0,  # Will be updated as files are processed
                'current_chunk': 0
            }
        )
        
        # Get extractor configuration
        config = {}
        if provider:
            config['provider'] = provider
        if model:
            config['model'] = model
        if use_api is not None:
            config['use_api'] = use_api
        if temperature is not None:
            config['temperature'] = temperature
            
        print(f"[Extraction Task] Extractor configuration: {config}")
        
        # Create markdown directory path
        markdown_dir = Path(f"{DATA_DIR}/cached/{source}/{dataset_name}-md")
        os.makedirs(markdown_dir, exist_ok=True)
        print(f"[Extraction Task] Created markdown directory: {markdown_dir}")
        
        # STEP 1: Convert all PDF files to markdown first using multithreading
        print(f"\n[Extraction Task] ===== STEP 1: CONVERTING PDFs TO MARKDOWN (MULTITHREADED) =====")
        logger.info("Step 1: Converting PDF files to markdown (multithreaded)")
        markdown_cache = {}
        
        # Function to process a single PDF file
        def process_pdf_file(file_info):
            filename, index = file_info
            
            # Check if extraction has been paused or cancelled
            current_status = extraction_progress.get_extraction_status(source, dataset_name)
            if not current_status or current_status == 'cancelled':
                print(f"[Thread {threading.current_thread().name}] Extraction cancelled for {source}/{dataset_name}")
                logger.info(f"Extraction cancelled for {source}/{dataset_name}")
                return
            
            if current_status == 'paused':
                print(f"[Thread {threading.current_thread().name}] Extraction paused for {source}/{dataset_name}")
                logger.info(f"Extraction paused for {source}/{dataset_name}")
                return
            
            # Update the current file information
            print(f"[Thread {threading.current_thread().name}] Starting processing of file {index+1}/{len(files)}: {filename}")
            extraction_progress.update_extraction_progress(
                source, 
                dataset_name, 
                {
                    'current_file': filename,
                    'current_file_index': index,
                    'file_progress': 0,
                    'message': 'Converting PDFs to markdown'
                }
            )
            
            logger.info(f"Processing file {index+1}/{len(files)}: {filename}")
            
            # Only process PDF files
            if filename.lower().endswith('.pdf'):
                # Check if markdown file already exists
                markdown_filename = os.path.splitext(os.path.basename(filename))[0] + '.md'
                markdown_file_path = markdown_dir / markdown_filename
                
                if markdown_file_path.exists():
                    print(f"[Thread {threading.current_thread().name}] Markdown file already exists for {filename}, loading from cache")
                    logger.info(f"Markdown file already exists for {filename}, loading from cache")
                    try:
                        with open(markdown_file_path, 'r', encoding='utf-8') as f:
                            markdown_content = f.read()
                            print(f"[Thread {threading.current_thread().name}] Successfully loaded markdown from cache, size: {len(markdown_content)} characters")
                        # Store content in cache
                        print(f"[Thread {threading.current_thread().name}] Storing content in cache")
                        markdown_cache[filename] = markdown_content
                        print(f"[Thread {threading.current_thread().name}] Content stored in cache")
                    except Exception as e:
                        print(f"[Thread {threading.current_thread().name}] ERROR loading cached markdown for {filename}: {str(e)}")
                        logger.error(f"Error loading cached markdown for {filename}: {str(e)}")
                        # If loading from cache fails, convert the PDF
                        try:
                            print(f"[Thread {threading.current_thread().name}] Cache loading failed, converting PDF instead")
                            markdown_content = convert_pdf_to_markdown(filename, source, dataset_name)
                            # Store content in cache
                            print(f"[Thread {threading.current_thread().name}] Storing content in cache after conversion")
                            markdown_cache[filename] = markdown_content
                            print(f"[Thread {threading.current_thread().name}] Content stored in cache")
                            print(f"[Thread {threading.current_thread().name}] Successfully converted {filename} to markdown")
                            logger.info(f"Successfully converted {filename} to markdown")
                        except Exception as e:
                            print(f"[Thread {threading.current_thread().name}] ERROR converting {filename} to markdown: {str(e)}")
                            logger.error(f"Error converting {filename} to markdown: {str(e)}", exc_info=True)
                            # Continue with the next file despite errors
                else:
                    # Convert PDF to markdown
                    try:
                        print(f"[Thread {threading.current_thread().name}] No cached markdown found, converting PDF to markdown")
                        markdown_content = convert_pdf_to_markdown(filename, source, dataset_name)
                        # Store content in cache
                        print(f"[Thread {threading.current_thread().name}] Storing content in cache after fresh conversion")
                        markdown_cache[filename] = markdown_content
                        print(f"[Thread {threading.current_thread().name}] Content stored in cache")
                        print(f"[Thread {threading.current_thread().name}] Successfully converted {filename} to markdown")
                        logger.info(f"Successfully converted {filename} to markdown")
                    except Exception as e:
                        print(f"[Thread {threading.current_thread().name}] ERROR converting {filename} to markdown: {str(e)}")
                        logger.error(f"Error converting {filename} to markdown: {str(e)}", exc_info=True)
                        # Continue with the next file despite errors
            print(f"[Thread {threading.current_thread().name}] Completed processing of file: {filename}")
        
        # Create threads for PDF processing
        pdf_files = [(filename, i) for i, filename in enumerate(files) if filename.lower().endswith('.pdf')]
        threads = []
        
        print(f"[Extraction Task] Found {len(pdf_files)} PDF files to process")
        
        # Determine the number of threads to use (limit to a reasonable number)
        max_threads = min(10, len(pdf_files))
        print(f"[Extraction Task] Using {max_threads} concurrent threads for PDF processing")
        
        # Create and start threads
        for i in range(0, len(pdf_files), max_threads):
            batch = pdf_files[i:i+max_threads]
            print(f"[Extraction Task] Starting batch of {len(batch)} files (batch {i//max_threads + 1})")
            
            for file_info in batch:
                thread = threading.Thread(target=process_pdf_file, args=(file_info,))
                threads.append(thread)
                print(f"[Extraction Task] Starting thread for file: {file_info[0]}")
                thread.start()
            
            # Wait for all threads in this batch to complete
            print(f"[Extraction Task] Waiting for all {len(threads)} threads in batch to complete")
            for thread in threads:
                thread.join()
            
            print(f"[Extraction Task] Batch {i//max_threads + 1} completed")
            
            # Check if extraction has been paused or cancelled
            current_status = extraction_progress.get_extraction_status(source, dataset_name)
            if not current_status or current_status == 'cancelled':
                print(f"[Extraction Task] Extraction cancelled for {source}/{dataset_name}")
                logger.info(f"Extraction cancelled for {source}/{dataset_name}")
                return
            
            if current_status == 'paused':
                print(f"[Extraction Task] Extraction paused for {source}/{dataset_name}")
                logger.info(f"Extraction paused for {source}/{dataset_name}")
                return
        
        print(f"[Extraction Task] PDF to markdown conversion phase complete. Converted {len(markdown_cache)} files.")
        print(f"[Extraction Task] Cached markdown files directory: {markdown_dir}")
        
        # STEP 2: Process all files with LLM extraction (single-threaded)
        print(f"\n[Extraction Task] ===== STEP 2: RUNNING LLM EXTRACTION (SINGLE-THREADED) =====")
        logger.info("Step 2: Running LLM extraction on all files (single-threaded)")
        
        for i, filename in enumerate(files):
            # Check if extraction has been paused or cancelled
            current_status = extraction_progress.get_extraction_status(source, dataset_name)
            
            if not current_status or current_status == 'cancelled':
                print(f"[Extraction Task] Extraction cancelled for {source}/{dataset_name}")
                logger.info(f"Extraction cancelled for {source}/{dataset_name}")
                return
            
            if current_status == 'paused':
                print(f"[Extraction Task] Extraction paused for {source}/{dataset_name} at file {i+1}/{len(files)}")
                logger.info(f"Extraction paused for {source}/{dataset_name} at file {i+1}/{len(files)}")
                return
            
            # Update the current file information
            print(f"[Extraction Task] Starting LLM extraction for file {i+1}/{len(files)}: {filename}")
            extraction_progress.update_extraction_progress(
                source, 
                dataset_name, 
                {
                    'current_file': filename,
                    'current_file_index': i,
                    'file_progress': 0,
                    'message': 'Running LLM extraction'
                }
            )
            
            logger.info(f"Processing file {i+1}/{len(files)}: {filename}")
            
            # Process the file
            try:
                # Use cached markdown content if available
                markdown_content = markdown_cache.get(filename)
                if markdown_content:
                    print(f"[Extraction Task] Using cached markdown content for {filename}, size: {len(markdown_content)} characters")
                else:
                    print(f"[Extraction Task] No cached markdown found for {filename}, will process as-is")
                
                result = process_file(filename, source, dataset_name, config, markdown_content)
                print(f"[Extraction Task] LLM extraction completed for {filename}")
                
                # Update processed files count
                print(f"[Extraction Task] Updating processed files count: {i+1}/{len(files)}")
                extraction_progress.update_extraction_progress(
                    source, 
                    dataset_name, 
                    {
                        'processed_files': i + 1
                    }
                )
                
            except Exception as e:
                print(f"[Extraction Task] ERROR processing file {filename}: {str(e)}")
                logger.error(f"Error processing file {filename}: {str(e)}", exc_info=True)
                # Continue with the next file despite errors
        
        # Complete the extraction
        print(f"\n[Extraction Task] All files processed, completing extraction task")
        extraction_progress.complete_extraction(
            source, 
            dataset_name, 
            True, 
            f"Successfully processed {len(files)} files"
        )
        
        print(f"[Extraction Task] Extraction task completed successfully")
        logger.info(f"Completed dataset extraction task with {len(files)} files")
    except Exception as e:
        print(f"[Extraction Task] ERROR in dataset extraction task: {str(e)}")
        logger.error(f"Error in dataset extraction task: {str(e)}", exc_info=True)
        
        # Mark extraction as failed
        print(f"[Extraction Task] Marking extraction as failed")
        extraction_progress.complete_extraction(
            source, 
            dataset_name, 
            False, 
            f"Error in extraction task: {str(e)}"
        )
        
        raise

def resume_extraction_task(extraction_record):
    """Resume an extraction from its current state"""
    try:
        source = extraction_record.source
        dataset_name = extraction_record.dataset_name
        
        logger.info(f"Resuming extraction for {source}/{dataset_name}")
        
        # Get the extraction data needed to resume
        files = extraction_record.get_files()
        schema = extraction_record.get_schema()
        current_file_index = extraction_record.current_file_index or 0
        
        # Get provider and model from the extraction record if available
        provider = extraction_record.provider
        model = extraction_record.model
        use_api = extraction_record.use_api
        temperature = extraction_record.temperature
        
        # Only pass the remaining files
        remaining_files = files[current_file_index:]
        logger.info(f"Resuming extraction with {len(remaining_files)} remaining files")
        
        # Extract output directory from message or construct it
        output_dir = f"{DATA_DIR}/extracted/{source}/{dataset_name}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Update status to in_progress
        extraction_progress.update_extraction_progress(
            source, 
            dataset_name, 
            {
                'status': 'in_progress',
                'message': 'Extraction resumed by batch processor'
            }
        )
        
        # Start the extraction process
        handle_dataset_extraction(
            extraction_record.id, 
            source, 
            dataset_name, 
            remaining_files, 
            schema, 
            output_dir, 
            provider, model, use_api, temperature
        )
        
        return True
    except Exception as e:
        logger.exception(f"Error resuming extraction task: {e}")
        
        # Mark extraction as failed
        extraction_progress.complete_extraction(
            source, 
            dataset_name, 
            False, 
            f"Error resuming extraction task: {str(e)}"
        )
        
        return False

def poll_for_extractions():
    """Poll the database for extractions that need to be processed"""
    try:
        logger.info("Polling for extractions that need to be processed")
        
        with db.get_session() as session:
            # Find scheduled extractions (highest priority)
            scheduled_extractions = session.query(ExtractionProgress).filter_by(
                status='scheduled'
            ).order_by(ExtractionProgress.updated_at.desc()).all()
            
            # Find paused extractions
            paused_extractions = session.query(ExtractionProgress).filter_by(
                status='paused'
            ).all()
            
            # Find in-progress extractions
            in_progress_extractions = session.query(ExtractionProgress).filter_by(
                status='in_progress'
            ).filter(
                ExtractionProgress.end_time.is_(None)  # Only get truly in-progress extractions
            ).all()
            
            # Combine them with scheduled extractions first (they take priority)
            pending_extractions = scheduled_extractions + paused_extractions + in_progress_extractions
            
            if not pending_extractions:
                logger.info("No pending extractions found")
                return
            
            logger.info(f"Found {len(pending_extractions)} pending extractions to process")
            
            # Process each pending extraction sequentially (single-threaded)
            for extraction in pending_extractions:
                # Check if this extraction is already being processed
                # We'll use a database lock to prevent duplicate processing
                with db.get_session() as check_session:
                    # Check if the extraction is still in a pending state
                    current_extraction = check_session.query(ExtractionProgress).get(extraction.id)
                    if not current_extraction or current_extraction.status not in ['scheduled', 'paused', 'in_progress']:
                        logger.info(f"Extraction {extraction.id} ({extraction.source}/{extraction.dataset_name}) is no longer pending, skipping")
                        continue
                    
                    # If it's in_progress, check if it has an end_time
                    if current_extraction.status == 'in_progress' and current_extraction.end_time is not None:
                        logger.info(f"Extraction {extraction.id} ({extraction.source}/{extraction.dataset_name}) is already completed, skipping")
                        continue
                
                logger.info(f"Starting processing for extraction {extraction.id} ({extraction.source}/{extraction.dataset_name})")
                
                # Process the extraction directly (single-threaded)
                resume_extraction_task(extraction)
    
    except Exception as e:
        logger.exception(f"Error polling for extractions: {e}")

def run_batch_processor(interval_seconds=60):
    """Run the batch processor continuously with the specified interval"""
    logger.info(f"Starting extraction batch processor with {interval_seconds}s polling interval")
    
    while True:
        try:
            poll_for_extractions()
        except Exception as e:
            logger.exception(f"Error in extraction batch processor: {e}")
        
        time.sleep(interval_seconds)

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the batch processor
    run_batch_processor() 