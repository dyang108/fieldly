import logging
from typing import Dict, Any, List, Optional
import time
import json
from pathlib import Path
import threading
import os

logger = logging.getLogger(__name__)

# Reference to the Flask-SocketIO instance
# This will be set from app.py
socketio = None

# Dictionary to store extraction state for each dataset
extraction_state = {}

# Lock for thread-safe access to extraction_state
extraction_state_lock = threading.Lock()

# State file directory for persistent state
STATE_DIR = Path(".extraction_state")

def init_socketio(socketio_instance):
    """
    Initialize the module with the Flask-SocketIO instance
    
    Args:
        socketio_instance: The Flask-SocketIO instance
    """
    global socketio
    socketio = socketio_instance
    logger.info("Extraction progress module initialized with SocketIO")
    
    # Ensure state directory exists
    STATE_DIR.mkdir(exist_ok=True, parents=True)
    
    # Load any existing states from disk
    _load_states_from_disk()

def _get_state_file_path(source: str, dataset_name: str) -> Path:
    """Get the path to the state file for a specific extraction"""
    room = f"{source}_{dataset_name}"
    return STATE_DIR / f"{room}.json"

def _save_state_to_disk(source: str, dataset_name: str) -> None:
    """Save extraction state to disk for persistence"""
    room = f"{source}_{dataset_name}"
    
    with extraction_state_lock:
        if room not in extraction_state:
            return
        
        try:
            state_file = _get_state_file_path(source, dataset_name)
            with open(state_file, 'w') as f:
                json.dump(extraction_state[room], f)
                
            logger.info(f"Saved extraction state to disk: {state_file}")
        except Exception as e:
            logger.error(f"Failed to save extraction state to disk: {e}")

def _load_state_from_disk(source: str, dataset_name: str) -> Optional[Dict[str, Any]]:
    """Load extraction state from disk"""
    try:
        state_file = _get_state_file_path(source, dataset_name)
        if not state_file.exists():
            return None
            
        with open(state_file, 'r') as f:
            state = json.load(f)
            
        logger.info(f"Loaded extraction state from disk: {state_file}")
        return state
    except Exception as e:
        logger.error(f"Failed to load extraction state from disk: {e}")
        return None

def _load_states_from_disk() -> None:
    """Load all extraction states from disk on startup"""
    try:
        if not STATE_DIR.exists():
            return
            
        for state_file in STATE_DIR.glob("*.json"):
            try:
                room = state_file.stem  # Remove .json extension
                
                with open(state_file, 'r') as f:
                    state = json.load(f)
                
                # If the extraction was in progress when the server stopped,
                # mark it as interrupted to prevent confusion
                if state.get('status') == 'in_progress':
                    state['status'] = 'interrupted'
                    state['message'] = 'Extraction was interrupted by server restart'
                    logger.warning(f"Marking extraction {room} as interrupted due to server restart")
                    
                    # Write the updated state back to disk
                    with open(state_file, 'w') as f:
                        json.dump(state, f)
                
                with extraction_state_lock:
                    extraction_state[room] = state
                    
                logger.info(f"Loaded extraction state from disk: {state_file}")
            except Exception as e:
                logger.error(f"Failed to load state file {state_file}: {e}")
    except Exception as e:
        logger.error(f"Failed to load extraction states from disk: {e}")

def start_extraction(source: str, dataset_name: str, files: List[str]):
    """
    Start tracking a new extraction process
    
    Args:
        source: The source of the dataset (e.g., 'local', 's3')
        dataset_name: The name of the dataset
        files: List of files to be processed
    """
    room = f"{source}_{dataset_name}"
    
    with extraction_state_lock:
        extraction_state[room] = {
            'status': 'in_progress',
            'total_files': len(files),
            'processed_files': 0,
            'current_file': files[0] if files else '',
            'file_progress': 0,
            'merged_data': {},
            'files': files,
            'start_time': time.time(),
            'total_chunks': 0,  # Total chunks across all files
            'processed_chunks': 0,  # Total chunks processed so far
            'current_file_chunks': 0,  # Number of chunks in current file
            'current_file_chunk': 0  # Current chunk being processed in current file
        }
    
    # Save state to disk for persistence
    _save_state_to_disk(source, dataset_name)
    
    if socketio:
        try:
            socketio.emit('extraction_state', extraction_state[room], room=room)
            logger.info(f"Started extraction tracking for {room} with {len(files)} files")
        except Exception as e:
            logger.error(f"Error sending extraction state: {str(e)}")
    else:
        logger.warning("SocketIO not initialized, can't emit start_extraction event")

def update_file_chunks(source: str, dataset_name: str, file_name: str, total_chunks: int):
    """
    Update the total chunks for a file when starting to process it
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        file_name: The name of the current file
        total_chunks: Total number of chunks in this file
    """
    room = f"{source}_{dataset_name}"
    
    with extraction_state_lock:
        if room not in extraction_state:
            # Try to load from disk if not in memory
            state = _load_state_from_disk(source, dataset_name)
            if state:
                extraction_state[room] = state
            else:
                logger.warning(f"No extraction state found for {room}")
                return
        
        state = extraction_state[room]
        state['current_file'] = file_name
        state['current_file_chunks'] = total_chunks
        state['total_chunks'] += total_chunks
        state['current_file_chunk'] = 0
        state['file_progress'] = 0
    
    # Save state to disk for persistence
    _save_state_to_disk(source, dataset_name)
    
    if socketio:
        try:
            socketio.emit('file_chunks_updated', {
                'current_file': file_name,
                'total_chunks': total_chunks
            }, room=room)
            logger.debug(f"Updated file chunks for {room}: {file_name} - {total_chunks} chunks")
        except Exception as e:
            logger.error(f"Error sending file chunks update: {str(e)}")

def update_chunk_progress(source: str, dataset_name: str, chunk_index: int):
    """
    Update progress when a chunk has been processed
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        chunk_index: Index of the current chunk (0-based)
    """
    room = f"{source}_{dataset_name}"
    
    with extraction_state_lock:
        if room not in extraction_state:
            # Try to load from disk if not in memory
            state = _load_state_from_disk(source, dataset_name)
            if state:
                extraction_state[room] = state
            else:
                logger.warning(f"No extraction state found for {room}")
                return
        
        state = extraction_state[room]
        state['current_file_chunk'] = chunk_index + 1
        state['processed_chunks'] += 1
        
        # Calculate progress based on chunks
        if state['current_file_chunks'] > 0:
            state['file_progress'] = state['current_file_chunk'] / state['current_file_chunks']
    
    # Save state to disk for persistence
    _save_state_to_disk(source, dataset_name)
    
    if socketio:
        try:
            socketio.emit('chunk_progress', {
                'current_chunk': chunk_index + 1,
                'current_file_total_chunks': state['current_file_chunks'],
                'file_progress': state['file_progress'],
                'total_processed_chunks': state['processed_chunks'],
                'overall_total_chunks': state['total_chunks']
            }, room=room)
            logger.debug(f"Updated chunk progress for {room}: chunk {chunk_index + 1}/{state['current_file_chunks']}")
        except Exception as e:
            logger.error(f"Error sending chunk progress: {str(e)}")

def update_file_progress(source: str, dataset_name: str, file_name: str, progress: float):
    """
    Update the progress for the current file being processed
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        file_name: The name of the current file
        progress: Progress value from 0 to 1
    """
    room = f"{source}_{dataset_name}"
    
    with extraction_state_lock:
        if room not in extraction_state:
            # Try to load from disk if not in memory
            state = _load_state_from_disk(source, dataset_name)
            if state:
                extraction_state[room] = state
            else:
                logger.warning(f"No extraction state found for {room}")
                return
        
        state = extraction_state[room]
        state['current_file'] = file_name
        state['file_progress'] = progress
    
    # Save state to disk for persistence
    _save_state_to_disk(source, dataset_name)
    
    if socketio:
        try:
            socketio.emit('extraction_progress', {
                'current_file': file_name,
                'file_progress': progress
            }, room=room)
            logger.debug(f"Updated file progress for {room}: {file_name} - {progress:.2f}")
        except Exception as e:
            logger.error(f"Error sending file progress: {str(e)}")
    else:
        logger.warning("SocketIO not initialized, can't emit update_file_progress event")

def update_merged_data(source: str, dataset_name: str, merged_data: Dict[str, Any]):
    """
    Update the merged data from the extraction process
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        merged_data: The current merged data
    """
    room = f"{source}_{dataset_name}"
    
    # Convert to string to check size
    merged_data_str = json.dumps(merged_data)
    data_size = len(merged_data_str)
    logger.info(f"Merged data size for {room}: {data_size} bytes")
    
    # If data is too large (over 1MB), we might need to truncate it
    # or only send essential fields to avoid socket.io issues
    max_size = 1024 * 1024  # 1MB
    if data_size > max_size:
        logger.warning(f"Merged data for {room} is very large ({data_size} bytes). This might cause issues with socket.io transmission.")
        # You could implement truncation here if needed
    
    with extraction_state_lock:
        if room not in extraction_state:
            # Try to load from disk if not in memory
            state = _load_state_from_disk(source, dataset_name)
            if state:
                extraction_state[room] = state
            else:
                logger.warning(f"No extraction state found for {room}")
                return
        
        state = extraction_state[room]
        state['merged_data'] = merged_data
    
    # Save state to disk for persistence
    _save_state_to_disk(source, dataset_name)
    
    if socketio:
        try:
            # Send a simplified version with just the keys and size
            # to avoid large payloads over socket.io
            simplified_data = {
                'dataSize': data_size,
                'keys': list(merged_data.keys()),
                'status': 'updated'
            }
            socketio.emit('merged_data_simplified', simplified_data, room=room)
            logger.info(f"Sent simplified merged data notification for {room}")
            
            # Also send the full data for clients that want it
            socketio.emit('merged_data', {'merged_data': merged_data}, room=room)
            logger.info(f"Sent full merged data for {room}")
            
        except Exception as e:
            logger.error(f"Error sending merged data: {str(e)}")
    else:
        logger.warning("SocketIO not initialized, can't emit merged_data event")

def update_merged_data_with_reasoning(source: str, dataset_name: str, merged_data: Dict[str, Any], reasoning_entry: Dict[str, Any]):
    """
    Update the merged data from the extraction process with merge reasoning information
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        merged_data: The current merged data
        reasoning_entry: Information about the reasoning behind merge decisions
    """
    room = f"{source}_{dataset_name}"
    
    # Convert to string to check size
    merged_data_str = json.dumps(merged_data)
    data_size = len(merged_data_str)
    logger.info(f"Merged data size for {room}: {data_size} bytes")
    
    # If data is too large (over 1MB), we might need to truncate it
    max_size = 1024 * 1024  # 1MB
    if data_size > max_size:
        logger.warning(f"Merged data for {room} is very large ({data_size} bytes). This might cause issues with socket.io transmission.")
    
    with extraction_state_lock:
        if room not in extraction_state:
            # Try to load from disk if not in memory
            state = _load_state_from_disk(source, dataset_name)
            if state:
                extraction_state[room] = state
            else:
                logger.warning(f"No extraction state found for {room}")
                return
        
        state = extraction_state[room]
        state['merged_data'] = merged_data
        
        # Initialize or update merge_reasoning_history
        if 'merge_reasoning_history' not in state:
            state['merge_reasoning_history'] = []
        
        # Add the new reasoning entry to the history
        state['merge_reasoning_history'].append(reasoning_entry)
    
    # Save state to disk for persistence
    _save_state_to_disk(source, dataset_name)
    
    if socketio:
        try:
            # Send a simplified version with just the keys and size
            simplified_data = {
                'dataSize': data_size,
                'keys': list(merged_data.keys()),
                'status': 'updated'
            }
            socketio.emit('merged_data_simplified', simplified_data, room=room)
            logger.info(f"Sent simplified merged data notification for {room}")
            
            # Send the full merged data
            socketio.emit('merged_data', {'merged_data': merged_data}, room=room)
            
            # Send the merge reasoning information separately
            socketio.emit('merge_reasoning', reasoning_entry, room=room)
            logger.info(f"Sent merge reasoning for {room}")
            
            # Also send the complete history for clients that might have missed updates
            socketio.emit('merge_reasoning_history', {
                'history': state.get('merge_reasoning_history', [])
            }, room=room)
            
        except Exception as e:
            logger.error(f"Error sending merged data with reasoning: {str(e)}")
    else:
        logger.warning("SocketIO not initialized, can't emit merge_reasoning event")

def file_completed(source: str, dataset_name: str, file_name: str):
    """
    Mark a file as completed and update the processed count
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        file_name: The name of the completed file
    """
    room = f"{source}_{dataset_name}"
    
    with extraction_state_lock:
        if room not in extraction_state:
            # Try to load from disk if not in memory
            state = _load_state_from_disk(source, dataset_name)
            if state:
                extraction_state[room] = state
            else:
                logger.warning(f"No extraction state found for {room}")
                return
        
        state = extraction_state[room]
        state['processed_files'] += 1
        
        # Set the next file as current if available
        files = state['files']
        current_index = files.index(file_name) if file_name in files else -1
        
        if current_index >= 0 and current_index + 1 < len(files):
            state['current_file'] = files[current_index + 1]
            state['file_progress'] = 0
        else:
            state['file_progress'] = 1
    
    # Save state to disk for persistence
    _save_state_to_disk(source, dataset_name)
    
    if socketio:
        try:
            socketio.emit('file_completed', {
                'completed_file': file_name,
                'processed_files': state['processed_files'],
                'total_files': state['total_files'],
                'next_file': state['current_file'] if state['processed_files'] < state['total_files'] else None
            }, room=room)
            logger.info(f"File completed for {room}: {file_name} - {state['processed_files']}/{state['total_files']}")
        except Exception as e:
            logger.error(f"Error sending file completed event: {str(e)}")
    else:
        logger.warning("SocketIO not initialized, can't emit file_completed event")

def complete_extraction(source: str, dataset_name: str, success: bool, message: str = ""):
    """
    Mark the extraction process as completed
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        success: Whether the extraction was successful
        message: Optional message with details about the extraction
    """
    room = f"{source}_{dataset_name}"
    
    with extraction_state_lock:
        if room not in extraction_state:
            # Try to load from disk if not in memory
            state = _load_state_from_disk(source, dataset_name)
            if state:
                extraction_state[room] = state
            else:
                logger.warning(f"No extraction state found for {room}")
                return
        
        state = extraction_state[room]
        state['status'] = 'completed' if success else 'failed'
        state['message'] = message
        state['end_time'] = time.time()
        state['duration'] = state['end_time'] - state['start_time']
    
    # Save state to disk for persistence
    _save_state_to_disk(source, dataset_name)
    
    if socketio:
        try:
            # Send a debug notification first
            socketio.emit('debug_message', {
                'message': f"Extraction completed for {room}: success={success}"
            }, room=room)
            logger.info(f"Sent debug notification for completion of {room}")
            
            # Then send the actual completion event
            socketio.emit('extraction_completed', {
                'success': success,
                'message': message,
                'processed_files': state['processed_files'],
                'total_files': state['total_files'],
                'duration': state['duration']
            }, room=room)
            logger.info(f"Extraction completed for {room}: success={success}, duration={state['duration']:.2f}s")
            
            # Also emit a general event for any listeners
            socketio.emit('extraction_status_change', {
                'room': room,
                'status': 'completed' if success else 'failed',
                'message': message
            }, room=room)
        except Exception as e:
            logger.error(f"Error sending extraction completed event: {str(e)}")
    else:
        logger.warning("SocketIO not initialized, can't emit complete_extraction event")

def get_extraction_state(source: str, dataset_name: str) -> Optional[Dict[str, Any]]:
    """
    Get the current extraction state for a dataset
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        
    Returns:
        The current extraction state or None if not found
    """
    room = f"{source}_{dataset_name}"
    
    with extraction_state_lock:
        # Check if state is in memory
        if room in extraction_state:
            return extraction_state[room]
        
        # Try to load from disk if not in memory
        state = _load_state_from_disk(source, dataset_name)
        if state:
            extraction_state[room] = state
            return state
            
        return None

def clear_extraction_state(source: str, dataset_name: str) -> None:
    """
    Clear the extraction state for a dataset when it's no longer needed
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
    """
    room = f"{source}_{dataset_name}"
    
    with extraction_state_lock:
        if room in extraction_state:
            logger.info(f"Clearing extraction state for {room}")
            del extraction_state[room]
        else:
            logger.warning(f"No extraction state found for {room} to clear")
    
    # Remove state file from disk
    try:
        state_file = _get_state_file_path(source, dataset_name)
        if state_file.exists():
            state_file.unlink()
            logger.info(f"Removed extraction state file: {state_file}")
    except Exception as e:
        logger.error(f"Error removing extraction state file: {e}")

def is_extraction_active(source: str, dataset_name: str) -> bool:
    """
    Check if an extraction is currently active for a dataset
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        
    Returns:
        True if the extraction is active, False otherwise
    """
    state = get_extraction_state(source, dataset_name)
    if not state:
        return False
        
    # An extraction is considered active if it's in progress
    return state.get('status') == 'in_progress'

def get_extraction_status(source: str, dataset_name: str) -> Optional[str]:
    """
    Get the status of an extraction job.
    
    Args:
        source: The source of the dataset
        dataset_name: The name of the dataset
        
    Returns:
        Optional[str]: The status of the extraction job, or None if not found
    """
    state = get_extraction_state(source, dataset_name)
    if state is None:
        return None
    return state.get('status') 