import os
import threading
import logging
import pickle
import time
from typing import Dict, Any, Tuple, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class TaskManager:
    """
    A singleton class to manage long-running tasks like extractions.
    TaskManager keeps track of running extractions and allows them to be paused/resumed.
    Task state is persisted to disk for recovery in case of server restart.
    """
    _instance = None
    _lock = threading.Lock()
    
    @classmethod
    def get_instance(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        # Make sure this is only called once
        if TaskManager._instance is not None:
            raise RuntimeError("TaskManager is a singleton, use get_instance() instead")
        
        # Dictionary of active extractions: {(source, dataset_name): task_info}
        self.active_extractions: Dict[Tuple[str, str], Dict[str, Any]] = {}
        
        # Dictionary of paused extractions: {(source, dataset_name): task_info}
        self.paused_extractions: Dict[Tuple[str, str], Dict[str, Any]] = {}
        
        # Thread locks for each extraction
        self.extraction_locks: Dict[Tuple[str, str], threading.Lock] = {}
        
        # State file path for persisting task state
        self.state_file_path = self._get_state_file_path()
        
        # Load persisted state if available
        self._load_state()
    
    def _get_state_file_path(self) -> Path:
        """Get the path to the state file."""
        # Get the app directory (one level up from the current file)
        app_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        state_dir = app_dir / 'data' / 'state'
        
        # Create the state directory if it doesn't exist
        os.makedirs(state_dir, exist_ok=True)
        
        return state_dir / 'task_manager_state.pkl'
    
    def _save_state(self):
        """Save the current state to disk."""
        try:
            state = {
                'active_extractions': self.active_extractions,
                'paused_extractions': self.paused_extractions,
                'timestamp': time.time()
            }
            
            with open(self.state_file_path, 'wb') as f:
                pickle.dump(state, f)
                
            logger.info(f"TaskManager state saved to {self.state_file_path}")
        except Exception as e:
            logger.error(f"Failed to save TaskManager state: {e}")
    
    def _load_state(self):
        """Load the state from disk if available."""
        if not os.path.exists(self.state_file_path):
            logger.info(f"No TaskManager state file found at {self.state_file_path}")
            return
        
        try:
            with open(self.state_file_path, 'rb') as f:
                state = pickle.load(f)
            
            self.active_extractions = state.get('active_extractions', {})
            self.paused_extractions = state.get('paused_extractions', {})
            
            # Recreate locks for all extractions
            for key in list(self.active_extractions.keys()) + list(self.paused_extractions.keys()):
                self.extraction_locks[key] = threading.Lock()
            
            logger.info(f"Loaded TaskManager state from {self.state_file_path}")
            logger.info(f"Active extractions: {len(self.active_extractions)}, Paused extractions: {len(self.paused_extractions)}")
        except Exception as e:
            logger.error(f"Failed to load TaskManager state: {e}")
            # Reset to empty state
            self.active_extractions = {}
            self.paused_extractions = {}
    
    def register_extraction(self, source: str, dataset_name: str, task_info: Dict[str, Any]) -> bool:
        """
        Register a new extraction task.
        
        Args:
            source: The data source name
            dataset_name: The dataset name
            task_info: Information about the task, including callback methods
                       Must include 'callback' method to be called for execution
        
        Returns:
            bool: True if registered successfully, False otherwise
        """
        key = (source, dataset_name)
        
        with self._lock:
            # Check if an extraction is already running for this dataset
            if key in self.active_extractions:
                logger.warning(f"Extraction already running for {source}/{dataset_name}")
                return False
            
            # Create a lock for this extraction
            if key not in self.extraction_locks:
                self.extraction_locks[key] = threading.Lock()
            
            # Register the extraction
            self.active_extractions[key] = task_info
            
            # Save state to disk
            self._save_state()
            
            # Start the extraction in a separate thread
            thread = threading.Thread(
                target=self._run_extraction,
                args=(source, dataset_name),
                daemon=True
            )
            thread.start()
            
            logger.info(f"Registered extraction for {source}/{dataset_name}")
            return True
    
    def _run_extraction(self, source: str, dataset_name: str):
        """
        Run an extraction in a separate thread.
        
        Args:
            source: The data source name
            dataset_name: The dataset name
        """
        key = (source, dataset_name)
        
        try:
            task_info = self.active_extractions.get(key)
            if not task_info:
                logger.error(f"No task info found for {source}/{dataset_name}")
                return
            
            callback = task_info.get('callback')
            if not callable(callback):
                logger.error(f"No valid callback found for {source}/{dataset_name}")
                return
            
            # Execute the callback with the task info
            callback(task_info)
            
            # When done, remove from active extractions
            with self._lock:
                if key in self.active_extractions:
                    del self.active_extractions[key]
                    self._save_state()
            
            logger.info(f"Extraction complete for {source}/{dataset_name}")
        except Exception as e:
            logger.exception(f"Error running extraction for {source}/{dataset_name}: {e}")
            
            # Mark as failed
            with self._lock:
                if key in self.active_extractions:
                    self.active_extractions[key]['status'] = 'failed'
                    self.active_extractions[key]['error'] = str(e)
                    self._save_state()
    
    def is_extraction_running(self, source: str, dataset_name: str) -> bool:
        """
        Check if an extraction is currently running for a dataset.
        
        Args:
            source: The data source name
            dataset_name: The dataset name
            
        Returns:
            bool: True if an extraction is running, False otherwise
        """
        key = (source, dataset_name)
        return key in self.active_extractions
    
    def get_extraction_info(self, source: str, dataset_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about an extraction.
        
        Args:
            source: The data source name
            dataset_name: The dataset name
            
        Returns:
            dict: Task information if found, None otherwise
        """
        key = (source, dataset_name)
        
        if key in self.active_extractions:
            return self.active_extractions[key]
        elif key in self.paused_extractions:
            return self.paused_extractions[key]
        else:
            return None
    
    def pause_extraction(self, source: str, dataset_name: str) -> bool:
        """
        Pause a running extraction.
        
        Args:
            source: The data source name
            dataset_name: The dataset name
            
        Returns:
            bool: True if paused successfully, False otherwise
        """
        key = (source, dataset_name)
        
        with self._lock:
            if key not in self.active_extractions:
                logger.warning(f"No active extraction found for {source}/{dataset_name}")
                return False
            
            # Set the pause flag in the task info
            self.active_extractions[key]['paused'] = True
            
            # Move from active to paused
            self.paused_extractions[key] = self.active_extractions[key]
            del self.active_extractions[key]
            
            # Save state to disk
            self._save_state()
            
            logger.info(f"Paused extraction for {source}/{dataset_name}")
            return True
    
    def resume_extraction(self, source: str, dataset_name: str) -> bool:
        """
        Resume a paused extraction.
        
        Args:
            source: The data source name
            dataset_name: The dataset name
            
        Returns:
            bool: True if resumed successfully, False otherwise
        """
        key = (source, dataset_name)
        
        with self._lock:
            if key not in self.paused_extractions:
                logger.warning(f"No paused extraction found for {source}/{dataset_name}")
                return False
            
            # Get the task info
            task_info = self.paused_extractions[key]
            
            # Remove the pause flag
            task_info['paused'] = False
            
            # Move from paused to active
            self.active_extractions[key] = task_info
            del self.paused_extractions[key]
            
            # Save state to disk
            self._save_state()
            
            # Resume the extraction in a separate thread
            thread = threading.Thread(
                target=self._run_extraction,
                args=(source, dataset_name),
                daemon=True
            )
            thread.start()
            
            logger.info(f"Resumed extraction for {source}/{dataset_name}")
            return True
    
    def cancel_extraction(self, source: str, dataset_name: str) -> bool:
        """
        Cancel an extraction (running or paused).
        
        Args:
            source: The data source name
            dataset_name: The dataset name
            
        Returns:
            bool: True if cancelled successfully, False otherwise
        """
        key = (source, dataset_name)
        
        with self._lock:
            if key in self.active_extractions:
                # Set the cancel flag in the task info
                self.active_extractions[key]['cancelled'] = True
                del self.active_extractions[key]
                self._save_state()
                logger.info(f"Cancelled active extraction for {source}/{dataset_name}")
                return True
            elif key in self.paused_extractions:
                # Remove from paused extractions
                del self.paused_extractions[key]
                self._save_state()
                logger.info(f"Cancelled paused extraction for {source}/{dataset_name}")
                return True
            else:
                logger.warning(f"No extraction found to cancel for {source}/{dataset_name}")
                return False
    
    def get_all_extractions(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all extractions (active and paused).
        
        Returns:
            dict: Dictionary of all extractions
                  {
                      'active': {(source, dataset_name): task_info, ...},
                      'paused': {(source, dataset_name): task_info, ...}
                  }
        """
        return {
            'active': self.active_extractions.copy(),
            'paused': self.paused_extractions.copy()
        } 