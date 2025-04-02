import logging
import os
import json
import subprocess
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app
from typing import Dict, List, Any, Optional

from db import db, Schema, DatasetSchemaMapping
from storage import create_storage
from ai import create_schema_generator

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
        logger.info(f"Processing file: {filename}")
        
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
            
            # 2. Extract data from Markdown based on schema
            logger.info(f"Extracting data from Markdown using schema")
            extracted_data = extract_data_from_markdown(str(md_path), schema)
            
            # 3. Save extracted data as JSON
            with open(json_path, 'w') as f:
                json.dump(extracted_data, f, indent=2)
                
            logger.info(f"Saved extracted data to: {json_path}")
            
            return {
                'filename': filename,
                'status': 'success',
                'output_file': str(json_path)
            }
        else:
            # For S3 storage (would require more complex implementation)
            # For now, we'll return an error
            logger.error("S3 storage not yet supported for extraction pipeline")
            return {
                'filename': filename,
                'status': 'error',
                'message': 'S3 storage not yet supported for extraction pipeline'
            }
            
    except Exception as e:
        logger.error(f"Error processing file {filename}: {str(e)}", exc_info=True)
        return {
            'filename': filename,
            'status': 'error',
            'message': str(e)
        }


def convert_pdf_to_markdown(pdf_path: str, md_path: str) -> None:
    """Convert a PDF file to Markdown using a PDF processing tool"""
    try:
        # This is where you'd call your PDF to markdown conversion
        # For now, let's use a placeholder that would call an external tool
        
        # Example using subprocess (assuming a tool like pdf2md exists)
        # subprocess.run(['pdf2md', pdf_path, md_path], check=True)
        
        # Simple implementation using pypdf and writing basic markdown
        import pypdf
        
        with open(pdf_path, 'rb') as f:
            pdf = pypdf.PdfReader(f)
            text = ""
            for page in pdf.pages:
                text += page.extract_text() + "\n\n"
        
        # Basic conversion to markdown format
        markdown = f"# {os.path.basename(pdf_path)}\n\n{text}"
        
        with open(md_path, 'w') as f:
            f.write(markdown)
            
        logger.info(f"Converted PDF to Markdown: {md_path}")
        
    except Exception as e:
        logger.error(f"Error converting PDF to Markdown: {str(e)}", exc_info=True)
        raise


def extract_data_from_markdown(md_path: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    """Extract structured data from a Markdown file based on a schema using an LLM"""
    try:
        # Read the markdown file
        with open(md_path, 'r') as f:
            content = f.read()
            
        # Get AI configuration from app config
        ai_type = None
        ai_config = {}
        
        # Check if local model should be used
        use_local_model = current_app.config.get('USE_LOCAL_MODEL', 'false').lower() == 'true'
        
        if use_local_model:
            ai_type = 'deepseek_local'
            ai_config = {
                'model': current_app.config.get('OLLAMA_MODEL', 'deepseek-r1:14b'),
                'api_url': current_app.config.get('OLLAMA_API_URL', 'http://localhost:11434/api/chat')
            }
        elif current_app.config.get('DEEPSEEK_API_KEY'):
            ai_type = 'deepseek_api'
            ai_config = {
                'api_key': current_app.config.get('DEEPSEEK_API_KEY'),
                'api_url': current_app.config.get('DEEPSEEK_API_URL', 'https://api.deepseek.com/v1/chat/completions')
            }
        else:
            ai_type = 'mock'
            
        # Create schema generator
        schema_generator = create_schema_generator(ai_type, ai_config)
        
        # Prepare a conversation to extract data
        conversation = [
            {"role": "user", "content": f"""
I have a document with the following content:

{content[:8000]}  # Truncate to avoid token limits

Please extract the data according to this JSON schema:

{json.dumps(schema, indent=2)}

Return ONLY the JSON data that follows the schema. Do not include any explanations, only return valid JSON.
"""}
        ]
        
        # Generate structured data
        result = schema_generator.generate_schema(conversation)
        
        # The result should be a JSON object matching the schema
        if isinstance(result, dict) and 'schema' in result:
            return result['schema']
        else:
            return result
            
    except Exception as e:
        logger.error(f"Error extracting data from Markdown: {str(e)}", exc_info=True)
        raise 