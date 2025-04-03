import logging
from flask import Blueprint, request, jsonify, current_app

from db import db, Schema, DatasetSchemaMapping
from storage import create_storage

logger = logging.getLogger(__name__)

datasets_bp = Blueprint('datasets', __name__, url_prefix='/api')


@datasets_bp.route('/datasets', methods=['GET'])
def get_datasets():
    """Get all datasets from storage"""
    try:
        logger.info("Starting GET /api/datasets request")
        
        # Get storage configuration from app config
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
        
        # Get datasets from storage
        local_datasets = []
        s3_datasets = []
        
        if storage_type == 'local':
            local_datasets = storage.list_datasets()
            logger.info(f"Found {len(local_datasets)} local datasets")
        elif storage_type == 's3':
            s3_datasets = storage.list_datasets()
            logger.info(f"Found {len(s3_datasets)} S3 datasets")
            
        result = {
            "local": local_datasets,
            "s3": s3_datasets
        }
        
        logger.info("Successfully prepared datasets response")
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in GET /api/datasets: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@datasets_bp.route('/dataset/<source>/<path:dataset_name>/files', methods=['GET'])
def get_dataset_files(source, dataset_name):
    """Get all files in a dataset"""
    try:
        logger.info(f"Starting GET /api/dataset/{source}/{dataset_name}/files request")
        
        # Get storage configuration from app config
        storage_config = {}
        
        if source == 's3':
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
        storage = create_storage(source, storage_config)
        
        # Get files from storage
        files = storage.list_files(dataset_name)
        logger.info(f"Found {len(files)} files in dataset {dataset_name}")
        
        return jsonify({
            'dataset': dataset_name,
            'source': source,
            'files': files
        })
    except Exception as e:
        logger.error(f"Error in GET /api/dataset/{source}/{dataset_name}/files: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@datasets_bp.route('/dataset-mappings', methods=['GET'])
def get_dataset_mappings():
    """Get all dataset-schema mappings"""
    session = db.get_session()
    try:
        logger.info("Starting GET /api/dataset-mappings request")
        mappings = session.query(DatasetSchemaMapping).all()
        logger.info(f"Successfully retrieved {len(mappings)} dataset mappings from database")
        
        result = []
        for mapping in mappings:
            schema_name = None
            if mapping.schema_id:
                schema = session.query(Schema).get(mapping.schema_id)
                if schema:
                    schema_name = schema.name
                    
            mapping_dict = {
                'id': mapping.id,
                'dataset_name': mapping.dataset_name,
                'source': mapping.source,
                'schema_id': mapping.schema_id,
                'schema_name': schema_name,
                'created_at': mapping.created_at.isoformat() if mapping.created_at else None
            }
            result.append(mapping_dict)
            
        logger.info("Successfully prepared dataset mappings response")
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in GET /api/dataset-mappings: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)


@datasets_bp.route('/dataset-mappings', methods=['POST'])
def create_or_update_mapping():
    """Create or update a dataset-schema mapping"""
    session = db.get_session()
    try:
        logger.info("Starting POST /api/dataset-mappings request")
        data = request.get_json()
        logger.debug(f"Received data: {data}")
        
        if not data or 'dataset_name' not in data or 'source' not in data:
            logger.error("Missing required fields in request data")
            return jsonify({'error': 'Missing required fields'}), 400
            
        # Check if mapping already exists
        existing_mapping = session.query(DatasetSchemaMapping).filter_by(
            dataset_name=data['dataset_name'],
            source=data['source']
        ).first()
        
        if existing_mapping:
            # Update existing mapping
            existing_mapping.schema_id = data.get('schema_id')
            logger.info(f"Updated mapping for dataset {data['dataset_name']}")
        else:
            # Create new mapping
            mapping = DatasetSchemaMapping(
                dataset_name=data['dataset_name'],
                source=data['source'],
                schema_id=data.get('schema_id')
            )
            session.add(mapping)
            logger.info(f"Created new mapping for dataset {data['dataset_name']}")
            
        session.commit()
        logger.info("Successfully saved dataset mapping")
        
        return jsonify({
            'success': True,
            'message': 'Dataset mapping saved successfully'
        }), 201
    except Exception as e:
        session.rollback()
        logger.error(f"Error in POST /api/dataset-mappings: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)


@datasets_bp.route('/apply-schema/<source>/<path:dataset_name>', methods=['POST'])
def apply_schema_to_dataset(source, dataset_name):
    """Apply a schema to a dataset"""
    session = db.get_session()
    try:
        logger.info(f"Starting POST /api/apply-schema/{source}/{dataset_name} request")
        data = request.get_json()
        
        if not data or 'schema_id' not in data:
            logger.error("Missing schema_id in request data")
            return jsonify({'error': 'schema_id is required'}), 400
            
        schema_id = data['schema_id']
        schema = session.query(Schema).get(schema_id)
        
        if not schema:
            logger.error(f"Schema with ID {schema_id} not found")
            return jsonify({'error': 'Schema not found'}), 404
            
        # TODO: Apply the schema to the dataset files
        # This would involve reading files from the dataset directory
        # and processing them according to the schema
        
        # For now, we'll just update or create the mapping
        existing_mapping = session.query(DatasetSchemaMapping).filter_by(
            dataset_name=dataset_name,
            source=source
        ).first()
        
        if existing_mapping:
            existing_mapping.schema_id = schema_id
        else:
            mapping = DatasetSchemaMapping(
                dataset_name=dataset_name,
                source=source,
                schema_id=schema_id
            )
            session.add(mapping)
            
        session.commit()
        
        return jsonify({
            'success': True,
            'message': f'Schema "{schema.name}" has been applied to dataset "{dataset_name}"'
        })
    except Exception as e:
        session.rollback()
        logger.error(f"Error applying schema to dataset: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)


@datasets_bp.route('/dataset-files/<source>/<path:dataset_name>', methods=['GET'])
def get_dataset_files_v2(source, dataset_name):
    """Get all files in a dataset"""
    try:
        logger.info(f"Starting GET /api/dataset-files/{source}/{dataset_name} request")
        
        # Get storage configuration from app config
        storage_config = {}
        
        if source == 's3':
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
        storage = create_storage(source, storage_config)
        
        # Get files from storage
        files = storage.list_files(dataset_name)
        logger.info(f"Found {len(files)} files in dataset {dataset_name}")
        
        return jsonify({
            'dataset': dataset_name,
            'source': source,
            'files': files
        })
    except Exception as e:
        logger.error(f"Error in GET /api/dataset-files/{source}/{dataset_name}: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@datasets_bp.route('/dataset-mapping/<source>/<path:dataset_name>', methods=['GET'])
def get_dataset_mapping(source, dataset_name):
    """Get dataset-schema mapping for a specific dataset"""
    session = db.get_session()
    try:
        logger.info(f"Starting GET /api/dataset-mapping/{source}/{dataset_name} request")
        
        # Find the mapping
        mapping = session.query(DatasetSchemaMapping).filter_by(
            dataset_name=dataset_name,
            source=source
        ).first()
        
        if not mapping:
            logger.info(f"No mapping found for dataset {dataset_name} (source: {source})")
            return jsonify({
                'dataset_name': dataset_name,
                'source': source,
                'schema_id': None,
                'schema_name': None
            })
        
        # Get schema name if available
        schema_name = None
        if mapping.schema_id:
            schema = session.query(Schema).get(mapping.schema_id)
            if schema:
                schema_name = schema.name
                
        result = {
            'id': mapping.id,
            'dataset_name': mapping.dataset_name,
            'source': mapping.source,
            'schema_id': mapping.schema_id,
            'schema_name': schema_name,
            'created_at': mapping.created_at.isoformat() if mapping.created_at else None
        }
        
        logger.info(f"Successfully retrieved mapping for dataset {dataset_name}")
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in GET /api/dataset-mapping/{source}/{dataset_name}: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session) 