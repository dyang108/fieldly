import logging
import time
import threading
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, cast
from flask import Blueprint, request, jsonify, Response
from sqlalchemy import desc

from db import db, ExtractionProgress
from utils import extraction_progress
from routes.extractors import handle_dataset_extraction

logger = logging.getLogger(__name__)

# Create a blueprint for extraction progress routes
extraction_progress_bp = Blueprint('extraction_progress', __name__, url_prefix='/api')

@extraction_progress_bp.route('/', methods=['GET'])
def list_extraction_progress() -> Tuple[Response, int]:
    """
    Get a list of all extraction progress records
    
    Returns:
        JSON response with all extraction progress records
    """
    try:
        session = db.get_session()
        progresses = session.query(ExtractionProgress).order_by(desc(ExtractionProgress.updated_at)).all()
        
        return jsonify({
            'extraction_progresses': [progress.to_dict() for progress in progresses]
        }), 200
    except Exception as e:
        logger.error(f"Error listing extraction progress: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)

@extraction_progress_bp.route('/<int:progress_id>', methods=['GET'])
def get_extraction_progress(progress_id: int) -> Tuple[Response, int]:
    """
    Get a specific extraction progress record by ID
    
    Args:
        progress_id: ID of the extraction progress record
        
    Returns:
        JSON response with the extraction progress record
    """
    try:
        session = db.get_session()
        progress = session.query(ExtractionProgress).get(progress_id)
        
        if not progress:
            return jsonify({'error': f'Extraction progress with ID {progress_id} not found'}), 404
            
        return jsonify(progress.to_dict()), 200
    except Exception as e:
        logger.error(f"Error getting extraction progress: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)

@extraction_progress_bp.route('/dataset/<source>/<path:dataset_name>', methods=['GET'])
def get_extraction_progress_by_dataset(source: str, dataset_name: str) -> Tuple[Response, int]:
    """
    Get extraction progress records for a specific dataset
    
    Args:
        source: Source of the dataset (e.g., 'local', 's3')
        dataset_name: Name of the dataset
        
    Returns:
        JSON response with extraction progress records for the dataset
    """
    try:
        session = db.get_session()
        progresses = session.query(ExtractionProgress).filter_by(
            source=source, 
            dataset_name=dataset_name
        ).order_by(desc(ExtractionProgress.updated_at)).all()
        
        # Return the most recent extraction progress, or a 404 if none found
        if progresses:
            return jsonify({
                'most_recent': progresses[0].to_dict(),
                'all_extractions': [progress.to_dict() for progress in progresses]
            }), 200
        else:
            return jsonify({
                'error': f'No extraction progress found for dataset {dataset_name} ({source})'
            }), 404
    except Exception as e:
        logger.error(f"Error getting extraction progress for dataset: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)

@extraction_progress_bp.route('/active', methods=['GET'])
def get_active_extractions() -> Tuple[Response, int]:
    """Get all active extractions"""
    try:
        # Query the database for in-progress extractions
        active_progresses = db.session.query(ExtractionProgress).filter(
            ExtractionProgress.status == 'in_progress',
            ExtractionProgress.end_time.is_(None)
        ).all()
        
        return jsonify({
            'active_extractions': [progress.to_dict() for progress in active_progresses]
        }), 200
    except Exception as e:
        logger.exception(f"Error getting active extractions: {e}")
        return jsonify({'error': str(e)}), 500

@extraction_progress_bp.route('/<int:progress_id>', methods=['DELETE'])
def delete_extraction_progress(progress_id: int) -> Tuple[Response, int]:
    """
    Delete a specific extraction progress record by ID
    
    Args:
        progress_id: ID of the extraction progress record
        
    Returns:
        JSON response indicating success or error
    """
    try:
        session = db.get_session()
        progress = session.query(ExtractionProgress).get(progress_id)
        
        if not progress:
            return jsonify({'error': f'Extraction progress with ID {progress_id} not found'}), 404
            
        session.delete(progress)
        session.commit()
            
        return jsonify({
            'success': True,
            'message': f'Extraction progress with ID {progress_id} deleted'
        }), 200
    except Exception as e:
        logger.error(f"Error deleting extraction progress: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)

@extraction_progress_bp.route('/check/<source>/<path:dataset_name>', methods=['GET'])
def check_active_extraction(source: str, dataset_name: str) -> Tuple[Response, int]:
    """
    Check if there is an active extraction for a specific dataset
    
    Args:
        source: Source of the dataset (e.g., 'local', 's3')
        dataset_name: Name of the dataset
        
    Returns:
        JSON response indicating whether an active extraction exists
    """
    try:
        # Use the extraction_progress module to check for active extractions
        is_active = extraction_progress.is_extraction_active(source, dataset_name)
        
        if is_active:
            extraction_state = extraction_progress.get_extraction_state(source, dataset_name)
            return jsonify({
                'active': True,
                'extraction_progress': extraction_state
            }), 200
        else:
            return jsonify({
                'active': False
            }), 200
    except Exception as e:
        logger.error(f"Error checking active extraction: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@extraction_progress_bp.route('/extraction-progress/list', methods=['GET'])
def list_extraction_progress_new():
    """Get a list of all extraction progress records."""
    try:
        with db.get_session() as session:
            progress_records = session.query(ExtractionProgress).order_by(desc(ExtractionProgress.start_time)).all()
            return jsonify({
                'success': True,
                'records': [record.to_dict() for record in progress_records]
            })
    except Exception as e:
        logger.exception(f"Error listing extraction progress records: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@extraction_progress_bp.route('/extraction-progress/dataset/<source>/<dataset_name>', methods=['GET'])
def get_extraction_progress_new(source, dataset_name):
    """Get extraction progress for a specific dataset."""
    try:
        with db.get_session() as session:
            progress_records = session.query(ExtractionProgress).filter_by(
                source=source,
                dataset_name=dataset_name
            ).order_by(desc(ExtractionProgress.start_time)).all()
            
            if not progress_records:
                return jsonify({'success': False, 'error': 'No extraction progress found'}), 404
            
            most_recent = progress_records[0].to_dict() if progress_records else None
            
            return jsonify({
                'success': True,
                'most_recent': most_recent,
                'records': [record.to_dict() for record in progress_records]
            })
    except Exception as e:
        logger.exception(f"Error getting extraction progress: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@extraction_progress_bp.route('/extraction-progress/create', methods=['POST'])
def create_extraction_progress():
    """Create a new extraction progress record."""
    try:
        data = request.json
        required_fields = ['dataset_name', 'source', 'status', 'total_files']
        
        for field in required_fields:
            if field not in data:
                return jsonify({'success': False, 'error': f'Missing required field: {field}'}), 400
        
        with db.get_session() as session:
            progress = ExtractionProgress(**data)
            session.add(progress)
            session.commit()
            
            return jsonify({
                'success': True,
                'id': progress.id,
                'message': 'Extraction progress record created'
            })
    except Exception as e:
        logger.exception(f"Error creating extraction progress: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@extraction_progress_bp.route('/extraction-progress/update/<int:progress_id>', methods=['PUT'])
def update_extraction_progress(progress_id):
    """Update an existing extraction progress record."""
    try:
        data = request.json
        
        with db.get_session() as session:
            progress = session.query(ExtractionProgress).filter_by(id=progress_id).first()
            if not progress:
                return jsonify({'success': False, 'error': 'Extraction progress not found'}), 404
            
            for key, value in data.items():
                if hasattr(progress, key):
                    setattr(progress, key, value)
            
            session.commit()
            
            return jsonify({
                'success': True,
                'message': 'Extraction progress updated'
            })
    except Exception as e:
        logger.exception(f"Error updating extraction progress: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@extraction_progress_bp.route('/extraction-status/<source>/<dataset_name>', methods=['GET'])
def get_extraction_status(source, dataset_name):
    """Check if an extraction is currently running for a dataset."""
    try:
        # Get the extraction status using our utility
        is_running = extraction_progress.is_extraction_active(source, dataset_name)
        extraction_state = extraction_progress.get_extraction_state(source, dataset_name)
        print(extraction_state)
        return jsonify({
            'success': True,
            'is_running': is_running,
            'extraction_info': extraction_state
        })
    except Exception as e:
        logger.exception(f"Error checking extraction status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@extraction_progress_bp.route('/extraction-resume/<source>/<dataset_name>', methods=['POST'])
def resume_extraction(source, dataset_name):
    """Resume a paused extraction process."""
    try:
        logger.info(f"Attempting to resume extraction for {source}/{dataset_name}")
        
        # Check if extraction is already active
        if extraction_progress.is_extraction_active(source, dataset_name):
            logger.warning(f"Extraction already running for {source}/{dataset_name}")
            return jsonify({
                'success': False,
                'error': 'Extraction is already running'
            }), 400
        
        # Find the paused extraction in the database
        with db.get_session() as session:
            paused_extraction = session.query(ExtractionProgress).filter_by(
                source=source,
                dataset_name=dataset_name,
                status='paused'
            ).order_by(desc(ExtractionProgress.start_time)).first()
            
            if not paused_extraction:
                logger.warning(f"No paused extraction found for {source}/{dataset_name}")
                return jsonify({
                    'success': False,
                    'error': 'No paused extraction found to resume'
                }), 404
            
            # Update status to in_progress
            paused_extraction.status = 'in_progress'
            paused_extraction.message = 'Extraction resumed'
            paused_extraction.updated_at = datetime.now()
            session.commit()
            
            # Get the extraction data needed to resume
            extraction_id = paused_extraction.id
            files = paused_extraction.get_files()
            schema = paused_extraction.get_schema()
            current_file_index = paused_extraction.current_file_index or 0
            
            # Only pass the remaining files
            remaining_files = files[current_file_index:]
            logger.info(f"Resuming extraction with {len(remaining_files)} remaining files")
            
            # Extract output directory from message or construct it
            import os
            from flask import current_app
            output_dir = f"{current_app.config['DATA_DIR']}/extracted/{source}/{dataset_name}"
            os.makedirs(output_dir, exist_ok=True)
            
            logger.info(f"Successfully updated extraction status in database for {source}/{dataset_name}")
        
        # Start extraction process in a separate thread
        extraction_thread = threading.Thread(
            target=handle_dataset_extraction,
            args=(extraction_id, source, dataset_name, remaining_files, schema, output_dir, None, None, None, None),
            daemon=True
        )
        extraction_thread.start()
        
        return jsonify({
            'success': True,
            'message': 'Extraction successfully resumed',
            'status': 'in_progress',
            'extraction': extraction_progress.get_extraction_state(source, dataset_name)
        })
            
    except Exception as e:
        logger.exception(f"Error resuming extraction: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@extraction_progress_bp.route('/extraction-pause/<source>/<dataset_name>', methods=['POST'])
def pause_extraction(source, dataset_name):
    """Pause a running extraction process."""
    try:
        logger.info(f"Attempting to pause extraction for {source}/{dataset_name}")
        
        # Check if an extraction is running
        if not extraction_progress.is_extraction_active(source, dataset_name):
            logger.warning(f"No active extraction found for {source}/{dataset_name}")
            return jsonify({
                'success': False,
                'error': 'No active extraction is running'
            }), 400
        
        # Update the extraction status in the database
        # The extraction thread will check the status and stop when it sees 'paused'
        extraction_progress.update_extraction_progress(
            source, 
            dataset_name, 
            {
                'status': 'paused',
                'message': 'Extraction paused by user'
            }
        )
        
        logger.info(f"Successfully paused extraction for {source}/{dataset_name}")
        
        return jsonify({
            'success': True,
            'message': 'Extraction successfully paused',
            'status': 'paused',
            'extraction': extraction_progress.get_extraction_state(source, dataset_name)
        })
            
    except Exception as e:
        logger.exception(f"Error pausing extraction: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@extraction_progress_bp.route('/extraction-progress/resume-extraction/<source>/<dataset_name>', methods=['POST', 'OPTIONS'])
def resume_extraction_route(source, dataset_name):
    """Resume a paused or in-progress extraction."""
    try:
        logger.info(f"Attempting to resume extraction for {source}/{dataset_name}")
        
        # Check if extraction is already active
        if extraction_progress.is_extraction_active(source, dataset_name):
            logger.warning(f"Extraction already running for {source}/{dataset_name}")
            extraction_state = extraction_progress.get_extraction_state(source, dataset_name)
            return jsonify({
                'success': True,
                'message': 'Extraction is already running',
                'extraction_info': extraction_state
            })
        
        # Find the extraction record to resume
        with db.get_session() as session:
            extraction_record = session.query(ExtractionProgress).filter_by(
                source=source,
                dataset_name=dataset_name,
                status='paused'
            ).order_by(desc(ExtractionProgress.start_time)).first()
            
            if not extraction_record:
                # Check for in-progress extractions as well
                extraction_record = session.query(ExtractionProgress).filter_by(
                    source=source,
                    dataset_name=dataset_name,
                    status='in_progress'
                ).order_by(desc(ExtractionProgress.start_time)).first()
            
            if not extraction_record:
                logger.warning(f"No paused or in-progress extraction found for {source}/{dataset_name}")
                return jsonify({
                    'success': False,
                    'error': f'No paused or in-progress extraction found for {dataset_name}'
                }), 404
            
            # Update status to scheduled (will be picked up by batch processor)
            extraction_record.status = 'scheduled'
            extraction_record.message = 'Extraction scheduled for resumption'
            extraction_record.updated_at = datetime.now()
            session.commit()
            extraction_id = extraction_record.id
        
        logger.info(f"Successfully scheduled extraction {extraction_id} for resumption")
        
        # Get the current extraction state
        extraction_state = extraction_progress.get_extraction_state(source, dataset_name)
        
        return jsonify({
            'success': True,
            'message': 'Extraction scheduled for resumption',
            'extraction_id': extraction_id,
            'extraction_info': extraction_state
        })
    except Exception as e:
        logger.exception(f"Error resuming extraction: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500 