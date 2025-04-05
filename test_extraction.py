#!/usr/bin/env python3
"""Test script for extraction process to verify our fix"""

import os
import sys
import json
from datetime import datetime
from pathlib import Path

# Import necessary modules
from app import app
from db import db, ExtractionProgress

def test_extraction_record():
    """Test creating an extraction progress record with files list"""
    with app.app_context():
        print("Creating test extraction progress record...")
        
        # Create a session
        session = db.get_session()
        
        try:
            # Create a test record
            test_files = ["test1.pdf", "test2.pdf", "test3.pdf"]
            
            # Create a new extraction progress record
            record = ExtractionProgress(
                dataset_name="test_dataset",
                source="local",
                status="starting",
                message="Test extraction",
                total_files=len(test_files),
                processed_files=0,
                start_time=datetime.now()
            )
            
            # Use the setter method to set the files list
            print("Setting files list using setter method...")
            record.set_files(test_files)
            
            # Add and commit the record
            session.add(record)
            session.commit()
            
            # Get the record ID
            record_id = record.id
            print(f"Created record with ID {record_id}")
            
            # Retrieve the record to verify it was saved correctly
            retrieved_record = session.query(ExtractionProgress).get(record_id)
            retrieved_files = retrieved_record.get_files()
            
            print(f"Retrieved files: {retrieved_files}")
            
            # Clean up
            session.delete(retrieved_record)
            session.commit()
            
            print("Test completed successfully!")
            return True
            
        except Exception as e:
            print(f"Error: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            db.close_session(session)

if __name__ == "__main__":
    test_extraction_record() 