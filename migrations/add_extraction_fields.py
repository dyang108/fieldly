#!/usr/bin/env python3
"""
Migration script to add provider, model, use_api, and temperature fields to the ExtractionProgress table
"""
import os
import sys
import logging
from sqlalchemy import text, inspect

# Add the parent directory to the path so we can import the db module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_migration():
    """Run the migration to add new fields to the ExtractionProgress table"""
    logger.info("Starting migration to add new fields to ExtractionProgress table")
    
    try:
        with db.get_session() as session:
            # Get the inspector to check existing columns
            inspector = inspect(db.engine)
            columns = [col['name'] for col in inspector.get_columns('extraction_progress')]
            
            # Add provider column if it doesn't exist
            if 'provider' not in columns:
                logger.info("Adding provider column")
                session.execute(text("""
                    ALTER TABLE extraction_progress 
                    ADD COLUMN provider VARCHAR(50)
                """))
            
            # Add model column if it doesn't exist
            if 'model' not in columns:
                logger.info("Adding model column")
                session.execute(text("""
                    ALTER TABLE extraction_progress 
                    ADD COLUMN model VARCHAR(100)
                """))
            
            # Add use_api column if it doesn't exist
            if 'use_api' not in columns:
                logger.info("Adding use_api column")
                session.execute(text("""
                    ALTER TABLE extraction_progress 
                    ADD COLUMN use_api BOOLEAN
                """))
            
            # Add temperature column if it doesn't exist
            if 'temperature' not in columns:
                logger.info("Adding temperature column")
                session.execute(text("""
                    ALTER TABLE extraction_progress 
                    ADD COLUMN temperature FLOAT
                """))
            
            session.commit()
            logger.info("Migration completed successfully")
            
    except Exception as e:
        logger.error(f"Error running migration: {e}")
        raise

if __name__ == "__main__":
    run_migration() 