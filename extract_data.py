#!/usr/bin/env python3
"""
Data extraction command-line script for SchemaGen

This script extracts structured data from PDF files in a dataset directory
according to a schema defined in the database.

Usage:
    python extract_data.py <dataset_name> [--source <source>]

Arguments:
    dataset_name    Name of the dataset directory in .data folder
    --source        Data source (default: local)
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from flask import Flask

from db import init_db, db, Schema, DatasetSchemaMapping
from storage import create_storage
from routes.extractors import process_file
from constants import MODEL_CONFIGS, DEFAULT_OLLAMA_HOST, DEFAULT_OLLAMA_API_PATH, DEFAULT_DATABASE_NAME

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def create_app():
    """Create a Flask app for configuration"""
    app = Flask(__name__)
    
    # Load configuration from environment variables
    app.config.update(
        # Storage configuration
        STORAGE_TYPE=os.getenv('STORAGE_TYPE', 'local'),  # 'local' or 's3'
        LOCAL_STORAGE_PATH=os.getenv('LOCAL_STORAGE_PATH', '.data'),
        
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
    
    return app

def extract_dataset(dataset_name, source='local'):
    """Extract data from a dataset according to its schema"""
    app = create_app()
    
    # Create app context for current_app usage in library code
    with app.app_context():
        session = db.get_session()
        try:
            logger.info(f"Starting extraction for dataset: {dataset_name} (source: {source})")
            
            # Initialize database
            init_db(app.config['DATABASE_URL'], drop_first=False)
            
            # Get storage configuration
            storage_type = app.config.get('STORAGE_TYPE', 'local')
            storage_config = {}
            
            if storage_type == 's3':
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
            storage = create_storage(storage_type, storage_config)
            
            # Get dataset mapping to find schema
            mapping = session.query(DatasetSchemaMapping).filter_by(
                dataset_name=dataset_name,
                source=source
            ).first()
            
            if not mapping or not mapping.schema_id:
                logger.error(f"No schema associated with dataset {dataset_name}")
                return False
            
            # Get schema
            schema = session.query(Schema).get(mapping.schema_id)
            if not schema:
                logger.error(f"Schema with ID {mapping.schema_id} not found")
                return False
                
            logger.info(f"Using schema: {schema.name} (ID: {schema.id})")
            
            # Get files in dataset
            files = storage.list_files(dataset_name)
            if not files:
                logger.error(f"No files found in dataset {dataset_name}")
                return False
                
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
                
                # Print result
                status = result.get('status', 'unknown')
                output_file = result.get('output_file', 'N/A')
                if status == 'success':
                    logger.info(f"✅ {filename}: Extracted to {output_file}")
                else:
                    logger.error(f"❌ {filename}: {result.get('message', 'Unknown error')}")
                
            logger.info(f"Extraction complete. Processed {len(results)} files.")
            return True
                
        except Exception as e:
            logger.error(f"Error in extraction process: {str(e)}", exc_info=True)
            return False
        finally:
            db.close_session(session)


def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description='Extract structured data from PDF files')
    parser.add_argument('dataset_name', help='Name of the dataset directory in .data folder')
    parser.add_argument('--source', default='local', help='Data source (default: local)')
    
    args = parser.parse_args()
    
    success = extract_dataset(args.dataset_name, args.source)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 