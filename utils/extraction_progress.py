import logging
from typing import Dict, Any, List, Optional
import time
import json
from pathlib import Path
import threading
import os
from datetime import datetime

from db import db, ExtractionProgress

logger = logging.getLogger(__name__)

# Lock for thread-safe access to database operations
db_lock = threading.Lock()

def is_extraction_active(source: str, dataset_name: str) -> bool:
    """
    Check if an extraction is currently active for a dataset
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        
    Returns:
        True if the extraction is active, False otherwise
    """
    with db.get_session() as session:
        active_extraction = session.query(ExtractionProgress).filter_by(
            source=source,
            dataset_name=dataset_name,
            status='in_progress'
        ).filter(
            ExtractionProgress.end_time.is_(None)  # Only get truly in-progress extractions
        ).first()

        if active_extraction:
            logger.info(f"Found active extraction in database for {source}/{dataset_name}")
            return True
        
        return False

def get_extraction_state(source: str, dataset_name: str) -> Optional[Dict[str, Any]]:
    """
    Get the current extraction state for a dataset
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        
    Returns:
        The current extraction state or None if not found
    """
    with db.get_session() as session:
        extraction_record = session.query(ExtractionProgress).filter_by(
            source=source,
            dataset_name=dataset_name
        ).order_by(ExtractionProgress.id.desc()).first()
        
        if extraction_record:
            return extraction_record.to_dict()
        
        return None

def start_extraction(source: str, dataset_name: str, files: List[str]) -> int:
    """
    Start tracking a new extraction process
    
    Args:
        source: The source of the dataset (e.g., 'local', 's3')
        dataset_name: The name of the dataset
        files: List of files to be processed
        
    Returns:
        The ID of the created extraction progress record
    """
    logger.info(f"Starting extraction for {source}/{dataset_name} with {len(files)} files")
    
    with db.get_session() as session:
        # Check if there's an active extraction
        active_extraction = session.query(ExtractionProgress).filter_by(
            source=source,
            dataset_name=dataset_name,
            status='in_progress'
        ).filter(
            ExtractionProgress.end_time.is_(None)  # Only get truly in-progress extractions
        ).first()
        
        if active_extraction:
            logger.warning(f"Active extraction already exists for {source}/{dataset_name}")
            return active_extraction.id
        
        # Create a new extraction record
        extraction = ExtractionProgress(
            source=source,
            dataset_name=dataset_name,
            status='in_progress',
            total_files=len(files),
            processed_files=0,
            current_file=files[0] if files else '',
            current_file_index=0,
            file_progress=0,
            files=json.dumps(files),
            total_chunks=0,
            processed_chunks=0,
            current_chunk=0,
            start_time=datetime.now()
        )
        
        session.add(extraction)
        session.commit()
        logger.info(f"Created new extraction progress record with id {extraction.id}")
        
        return extraction.id

def update_extraction_progress(
    source: str, 
    dataset_name: str, 
    update_data: Dict[str, Any]
) -> bool:
    """
    Update the extraction progress with new data
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        update_data: Dictionary with updates to apply, can include:
            - current_file: Current file being processed
            - file_progress: Progress on current file (0-1)
            - processed_files: Number of processed files
            - total_chunks: Total chunks for extraction
            - current_chunk: Current chunk being processed
            - processed_chunks: Number of processed chunks
            - merged_data: Updated merged data (JSON serializable)
            - merge_reasoning_history: Updated reasoning history
            - schema: Updated schema
            - status: New status ('in_progress', 'paused', 'completed', 'failed', 'cleared')
            - message: Status message or details
    
    Returns:
        True if update was successful, False otherwise
    """
    try:
        with db.get_session() as session:
            extraction = session.query(ExtractionProgress).filter_by(
                source=source,
                dataset_name=dataset_name,
                status='in_progress'
            ).filter(
                ExtractionProgress.end_time.is_(None)  # Only get truly in-progress extractions
            ).first()
            
            if not extraction:
                logger.warning(f"No active extraction found for {source}/{dataset_name}")
                return False
            
            # Update fields if provided in update_data
            for field, value in update_data.items():
                if field == 'merged_data' and value is not None:
                    extraction.merged_data = json.dumps(value)
                elif field == 'merge_reasoning_history' and value is not None:
                    extraction.merge_reasoning_history = json.dumps(value)
                elif field == 'schema' and value is not None:
                    extraction.schema = json.dumps(value)
                elif field == 'files' and value is not None:
                    extraction.files = json.dumps(value)
                elif hasattr(extraction, field):
                    setattr(extraction, field, value)
            
            # If status is changing to completed or failed, set end_time
            if 'status' in update_data and update_data['status'] in ['completed', 'failed']:
                extraction.end_time = datetime.now()
                if extraction.start_time:
                    # Calculate duration in seconds
                    duration = (extraction.end_time - extraction.start_time).total_seconds()
                    extraction.duration = duration
                    logger.info(f"Extraction {source}/{dataset_name} {update_data['status']} in {duration:.2f} seconds")
            
            session.commit()
            logger.debug(f"Updated extraction progress for {source}/{dataset_name}")
            return True
            
    except Exception as e:
        logger.error(f"Error updating extraction progress: {e}")
        return False

def clear_extraction_state(source: str, dataset_name: str) -> None:
    """
    Clear the extraction state for a dataset when it's no longer needed
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
    """
    try:
        with db.get_session() as session:
            extraction_record = session.query(ExtractionProgress).filter_by(
                source=source,
                dataset_name=dataset_name
            ).order_by(ExtractionProgress.id.desc()).first()
            
            if extraction_record:
                logger.info(f"Clearing extraction state for {source}/{dataset_name}")
                extraction_record.status = 'cleared'
                session.commit()
                logger.info(f"Extraction state cleared for {source}/{dataset_name}")
            else:
                logger.warning(f"No extraction state found for {source}/{dataset_name} to clear")
    except Exception as e:
        logger.error(f"Error clearing extraction state: {e}")

def get_extraction_status(source: str, dataset_name: str) -> Optional[str]:
    """
    Get the status of an extraction job.
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        
    Returns:
        Optional[str]: The status of the extraction job, or None if not found
    """
    with db.get_session() as session:
        extraction_record = session.query(ExtractionProgress).filter_by(
            source=source,
            dataset_name=dataset_name
        ).order_by(ExtractionProgress.id.desc()).first()
        
        if extraction_record:
            return extraction_record.status
        
        return None

def complete_extraction(source: str, dataset_name: str, success: bool, message: str = "") -> bool:
    """
    Mark the extraction process as completed
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        success: Whether the extraction was successful
        message: Optional message with details about the completion
        
    Returns:
        True if update was successful, False otherwise
    """
    status = 'completed' if success else 'failed'
    
    # Update the extraction record
    try:
        with db.get_session() as session:
            extraction = session.query(ExtractionProgress).filter_by(
                source=source,
                dataset_name=dataset_name,
                status='in_progress'
            ).filter(
                ExtractionProgress.end_time.is_(None)  # Only get truly in-progress extractions
            ).first()
            
            if extraction:
                extraction.status = status
                extraction.message = message
                extraction.end_time = datetime.now()
                if extraction.start_time:
                    duration = (extraction.end_time - extraction.start_time).total_seconds()
                    extraction.duration = duration
                    logger.info(f"Extraction {source}/{dataset_name} {status} in {duration:.2f} seconds")
                session.commit()
                logger.info(f"Updated extraction status to {status} for {source}/{dataset_name}")
                return True
            else:
                logger.warning(f"No active extraction found for {source}/{dataset_name} to complete")
                return False
    except Exception as e:
        logger.error(f"Error completing extraction: {e}")
        return False

def resume_extraction(source: str, dataset_name: str) -> Optional[int]:
    """
    Resume a paused or in-progress extraction
    
    This function is used to resume an extraction after a server restart
    or after it was paused.
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        
    Returns:
        The extraction ID if resumed successfully, None otherwise
    """
    logger.info(f"Attempting to resume extraction for {source}/{dataset_name}")
    
    with db.get_session() as session:
        # First check for scheduled extractions
        scheduled_extraction = session.query(ExtractionProgress).filter_by(
            source=source,
            dataset_name=dataset_name,
            status='scheduled'
        ).first()
        
        if scheduled_extraction:
            logger.info(f"Found scheduled extraction for {source}/{dataset_name}, resuming")
            return scheduled_extraction.id
            
        # Then check for paused extractions
        paused_extraction = session.query(ExtractionProgress).filter_by(
            source=source,
            dataset_name=dataset_name,
            status='paused'
        ).first()
        
        if paused_extraction:
            logger.info(f"Found paused extraction for {source}/{dataset_name}, resuming")
            paused_extraction.status = 'scheduled'
            session.commit()
            return paused_extraction.id
        
        # If no paused extraction, check for in-progress extractions
        # (this handles the case after a server restart)
        in_progress_extraction = session.query(ExtractionProgress).filter_by(
            source=source,
            dataset_name=dataset_name,
            status='in_progress'
        ).filter(
            ExtractionProgress.end_time.is_(None)  # Only get truly in-progress extractions
        ).first()
        
        if in_progress_extraction:
            logger.info(f"Found in-progress extraction for {source}/{dataset_name} after server restart, resuming")
            return in_progress_extraction.id
        
        logger.info(f"No paused or in-progress extraction found for {source}/{dataset_name}")
        return None

def delete_running_extraction(source: str, dataset_name: str) -> bool:
    """
    Delete a running extraction for a dataset
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        
    Returns:
        True if deletion was successful, False otherwise
    """
    try:
        with db.get_session() as session:
            # Find all running extractions (in_progress, scheduled, paused, or failed)
            extraction_records = session.query(ExtractionProgress).filter(
                ExtractionProgress.source == source,
                ExtractionProgress.dataset_name == dataset_name,
                ExtractionProgress.status.in_(['in_progress', 'scheduled', 'paused', 'failed'])
            ).all()
            
            if not extraction_records:
                logger.warning(f"No running extractions found for {source}/{dataset_name}")
                return False
            
            # Delete each running extraction
            for record in extraction_records:
                logger.info(f"Deleting extraction {record.id} for {source}/{dataset_name}")
                session.delete(record)
            
            session.commit()
            logger.info(f"Successfully deleted {len(extraction_records)} running extractions for {source}/{dataset_name}")
            return True
    except Exception as e:
        logger.error(f"Error deleting running extraction: {e}")
        return False 