#!/usr/bin/env python3
# No need for eventlet monkey patching anymore
import os
import logging
from typing import Optional, Dict, Any, Union, List, cast
from flask import Flask, request, jsonify, send_from_directory, Response, send_file
from flask_cors import CORS
import json
import time
from pathlib import Path

# All other imports
from db import init_db
from routes import register_blueprints
from constants import MODEL_CONFIGS, DEFAULT_OLLAMA_HOST, DEFAULT_OLLAMA_API_PATH, DEFAULT_DATABASE_NAME
from type_definitions import StorageType
from utils import extraction_progress
from utils.s3_utils import list_s3_buckets, list_s3_objects
from utils.schema_generator import generate_schema_from_file, merge_schemas
from utils.file_utils import get_file_type, is_supported_file_type, list_files_with_extensions
from storage import create_storage

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create Flask app
app: Flask = Flask(__name__,
    static_folder='frontend/dist',
    static_url_path='',
    template_folder='frontend/dist'
)

# Configure CORS
CORS(app, resources={
    r"/*": {
        "origins": ["http://localhost:5173", "http://localhost:5000"],
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Load configuration from environment variables
app.config.update(
    # Storage configuration
    STORAGE_TYPE=cast(StorageType, os.getenv('STORAGE_TYPE', 'local')),  # 'local' or 's3'
    LOCAL_STORAGE_PATH=os.getenv('LOCAL_STORAGE_PATH', '.data'),
    DATA_DIR=os.getenv('DATA_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), '.data')),
    
    # S3 configuration
    S3_BUCKET_NAME=os.getenv('S3_BUCKET_NAME', ''),
    AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID', ''),
    AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY', ''),
    AWS_REGION=os.getenv('AWS_REGION', ''),
    
    # AI configuration
    USE_LOCAL_MODEL=os.getenv('USE_LOCAL_MODEL', 'true'),
    OLLAMA_MODEL=MODEL_CONFIGS['deepseek']['local']['model'],
    OLLAMA_API_URL=f"{DEFAULT_OLLAMA_HOST}{DEFAULT_OLLAMA_API_PATH}",
    DEEPSEEK_API_KEY=os.getenv('DEEPSEEK_API_KEY', ''),
    DEEPSEEK_API_URL=os.getenv('DEEPSEEK_API_URL', 'https://api.deepseek.com/v1/chat/completions'),
    
    # Database configuration
    DATABASE_URL=f"sqlite:///{DEFAULT_DATABASE_NAME}"
)

# Initialize the database
init_db(app.config['DATABASE_URL'], drop_first=False)

# Register blueprints
register_blueprints(app)

# Handle general Flask errors
@app.errorhandler(Exception)
def handle_exception(e):
    """Handle general Flask exceptions"""
    logger.error(f"Flask exception: {str(e)}", exc_info=True)
    return jsonify({'error': str(e)}), 500

# Serve the React app root
@app.route('/')
def index() -> Response:
    return send_from_directory(app.static_folder, 'index.html')

# Handle client-side routing - this is crucial for SPAs with client-side routing
@app.route('/<path:path>')
def serve(path: str) -> Response:
    # Only attempt to serve actual files for api endpoints
    if path.startswith('api/'):
        # API routes are handled by blueprints, so 404 if hit here
        return jsonify({'error': 'API endpoint not found'}), 404
        
    # Check if this is a static asset request
    static_file_path = os.path.join(app.static_folder, path)
    if path != "" and os.path.exists(static_file_path) and not os.path.isdir(static_file_path):
        # Only serve actual files, not directories
        return send_from_directory(app.static_folder, path)
    
    # Otherwise, serve index.html for all non-API routes to enable client-side routing
    # This handles routes like /dataset/*, /extraction-progress/*, etc.
    return send_from_directory(app.static_folder, 'index.html')

# Add the ping endpoint for server reachability checks
@app.route('/api/ping', methods=['GET'])
def ping():
    """Simple ping endpoint to check if server is alive"""
    return jsonify({'status': 'ok', 'timestamp': int(time.time())})

# Add endpoint to check if extraction is active
@app.route('/api/extraction/status', methods=['GET'])
def check_extraction_status():
    """Check if an extraction is currently active for a dataset"""
    source = request.args.get('source')
    dataset_name = request.args.get('dataset_name')
    
    if not source or not dataset_name:
        return jsonify({'error': 'Missing source or dataset_name parameter'}), 400
    
    is_active = extraction_progress.is_extraction_active(source, dataset_name)
    
    return jsonify({
        'source': source,
        'dataset_name': dataset_name,
        'is_active': is_active
    })

# Add endpoint to get extraction state
@app.route('/api/extraction/state', methods=['GET'])
def get_extraction_state():
    """Get the current state of an extraction"""
    source = request.args.get('source')
    dataset_name = request.args.get('dataset_name')
    
    if not source or not dataset_name:
        return jsonify({'error': 'Missing source or dataset_name parameter'}), 400
    
    state = extraction_progress.get_extraction_state(source, dataset_name)
    
    if not state:
        return jsonify({
            'source': source,
            'dataset_name': dataset_name,
            'state': None,
            'is_active': False
        }), 404
    
    return jsonify({
        'source': source,
        'dataset_name': dataset_name,
        'state': state,
        'is_active': extraction_progress.is_extraction_active(source, dataset_name)
    })

# New endpoint to preview a file from a dataset
@app.route('/api/preview-file/<source>/<path:dataset_name>/<path:filename>', methods=['GET'])
def preview_file(source, dataset_name, filename):
    """Preview a file from a dataset - especially useful for PDFs"""
    try:
        logger.info(f"Preview file request for {source}/{dataset_name}/{filename}")
        
        # Get storage configuration
        storage_config = {}
        
        if source == 's3':
            storage_config = {
                'bucket_name': app.config.get('S3_BUCKET_NAME'),
                'aws_access_key_id': app.config.get('AWS_ACCESS_KEY_ID'),
                'aws_secret_access_key': app.config.get('AWS_SECRET_ACCESS_KEY'),
                'region_name': app.config.get('AWS_REGION')
            }
        else:
            storage_config = {
                'storage_path': app.config.get('LOCAL_STORAGE_PATH', '.data')
            }
        
        # Create storage instance
        storage = create_storage(source, storage_config)
        
        # Get file path
        if source == 'local':
            # For local files, build the path and serve directly
            file_path = os.path.join(app.config.get('LOCAL_STORAGE_PATH', '.data'), dataset_name, filename)
            logger.info(f"Serving local file: {file_path}")
            
            if not os.path.exists(file_path):
                return jsonify({'error': 'File not found'}), 404
                
            # Determine content type
            content_type = 'application/pdf' if filename.lower().endswith('.pdf') else None
            
            return send_file(file_path, mimetype=content_type)
        else:
            # For S3, download to temp file and serve
            temp_dir = Path('.temp')
            temp_dir.mkdir(exist_ok=True)
            
            temp_file = temp_dir / filename
            
            # Download file from S3
            storage.download_file(f"{dataset_name}/{filename}", str(temp_file))
            
            logger.info(f"Serving S3 file via temp: {temp_file}")
            
            if not temp_file.exists():
                return jsonify({'error': 'Failed to download file'}), 500
                
            # Determine content type
            content_type = 'application/pdf' if filename.lower().endswith('.pdf') else None
            
            return send_file(str(temp_file), mimetype=content_type)
            
    except Exception as e:
        logger.error(f"Error previewing file: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# New endpoint to fetch the content of an extracted JSON file
@app.route('/api/file-content', methods=['GET'])
def get_file_content():
    """Get the content of a file, particularly JSON files containing extraction results"""
    try:
        file_path = request.args.get('path')
        
        if not file_path:
            return jsonify({'error': 'Missing path parameter'}), 400
            
        logger.info(f"Fetching content for file: {file_path}")
        
        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 404
            
        # For JSON files, read and return the content
        if file_path.lower().endswith('.json'):
            with open(file_path, 'r') as f:
                content = json.load(f)
                
            return jsonify({
                'path': file_path,
                'content': content,
                'type': 'json'
            })
        else:
            # For other file types, return a small excerpt
            with open(file_path, 'r') as f:
                content = f.read(1024)  # First 1KB
                
            return jsonify({
                'path': file_path,
                'content': content,
                'type': 'text',
                'truncated': True
            })
            
    except Exception as e:
        logger.error(f"Error fetching file content: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# New endpoint to get available extraction results for a dataset
@app.route('/api/extraction-results/<source>/<path:dataset_name>', methods=['GET'])
def get_extraction_results(source, dataset_name):
    """Get all available extraction results for a dataset"""
    try:
        logger.info(f"Fetching extraction results for {source}/{dataset_name}")
        
        results = []
        results_dir = None
        directory = os.path.join('.data', f"{dataset_name}-extracted")
        print(directory)
        if os.path.exists(directory):
            logger.info(f"Found results directory at {directory}")
            results_dir = directory
            
            # Get the list of JSON files in this directory
            file_names = os.listdir(directory)
            
            for result_file in file_names:
                if result_file.endswith('.json'):
                    try:
                        # Get the original filename by removing .json extension
                        original_filename = result_file.replace('.json', '')
                        
                        # Create the output file path
                        output_file = os.path.join(directory, result_file)
                        
                        # Read the file content to check if it's valid
                        with open(output_file, 'r') as f:
                            content = json.load(f)
                        
                        # Add to results list
                        results.append({
                            'filename': original_filename,
                            'status': 'success',
                            'output_file': output_file
                        })
                    except Exception as e:
                        logger.error(f"Error processing result file {result_file}: {str(e)}")
                        results.append({
                            'filename': original_filename,
                            'status': 'error',
                            'message': f'Error reading extraction result: {str(e)}'
                        })
    
        if not results_dir:
            logger.info(f"No results directories found for {source}/{dataset_name}")
            results_dir = os.path.join('.data', 'cached', source, dataset_name)  # Default to cached location
        
        return jsonify({
            'source': source,
            'dataset_name': dataset_name,
            'results': results,
            'output_directory': results_dir,
            'processed_files': len(results)
        })
    except Exception as e:
        logger.error(f"Error fetching extraction results: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'source': source,
            'dataset_name': dataset_name,
            'results': []
        }), 500

# API endpoint to check for active extraction progress
@app.route('/api/extraction-progress/check/<source>/<path:dataset_name>', methods=['GET','OPTIONS'])
def check_extraction_progress(source: str, dataset_name: str):
    """Check if there's an active extraction progress for a dataset

    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset

    Returns:
        JSON response with active status and additional info
    """
    try:
        # Check if there's an extraction record in the database
        from db import db, ExtractionProgress
        with db.get_session() as session:
            extraction_record = session.query(ExtractionProgress).filter(
                ExtractionProgress.source == source,
                ExtractionProgress.dataset_name == dataset_name,
                ExtractionProgress.status.in_(['in_progress', 'scheduled', 'paused', 'failed'])
            ).order_by(ExtractionProgress.id.desc()).first()
            
            has_extraction_record = extraction_record is not None
            
            # Get the most recent record regardless of status
            most_recent = session.query(ExtractionProgress).filter_by(
                source=source,
                dataset_name=dataset_name
            ).order_by(ExtractionProgress.id.desc()).first()
        
        # Determine if there's an active extraction
        is_active = has_extraction_record
        
        response = {
            'active': is_active,
            'source': source,
            'dataset_name': dataset_name,
            'state': most_recent.to_dict() if most_recent else None,
            'has_db_record': has_extraction_record
        }
        
        logger.info(f"Checked extraction progress for {source}/{dataset_name}: active={is_active}")
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error checking extraction progress: {str(e)}", exc_info=True)
        return jsonify({
            'active': False,
            'error': str(e),
            'source': source,
            'dataset_name': dataset_name
        }), 500

if __name__ == '__main__':
    # Use regular Flask run instead of socketio.run
    app.run(
        debug=True,
        host='0.0.0.0',
        port=5000
    ) 