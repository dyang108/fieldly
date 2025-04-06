import logging
from typing import Optional, Any
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from .models import Base
from constants import DEFAULT_DATABASE_NAME

logger = logging.getLogger(__name__)


class Database:
    """Database connection and session management"""
    
    def __init__(self, database_url: str = f"sqlite:///{DEFAULT_DATABASE_NAME}"):
        """
        Initialize database connection
        
        Args:
            database_url: SQLAlchemy database URL
        """
        self.engine: Engine = create_engine(database_url)
        self.session_factory = sessionmaker(bind=self.engine)
        self.Session = scoped_session(self.session_factory)
    
    def create_tables(self, drop_first: bool = False, recreate_schema: bool = False) -> None:
        """
        Create all tables defined in models
        
        Args:
            drop_first: If True, drop all tables before creating
            recreate_schema: If True, update the schema without dropping data
        """
        if drop_first:
            logger.warning("Dropping all tables")
            Base.metadata.drop_all(self.engine)
            Base.metadata.create_all(self.engine)
        elif recreate_schema:
            logger.info("Updating database schema")
            # Import inspector to examine the database
            from sqlalchemy import inspect
            
            # Get an inspector to examine the database
            inspector = inspect(self.engine)
            
            # Get existing tables
            existing_tables = inspector.get_table_names()
            
            # For each table in the metadata
            for table_name, table in Base.metadata.tables.items():
                if table_name in existing_tables:
                    # Get existing columns
                    existing_columns = {col['name'] for col in inspector.get_columns(table_name)}
                    
                    # Add columns that don't exist yet
                    for column in table.columns:
                        if column.name not in existing_columns:
                            logger.info(f"Adding column {column.name} to table {table_name}")
                            column_type = column.type.compile(self.engine.dialect)
                            nullable = "NULL" if column.nullable else "NOT NULL"
                            default = f"DEFAULT {column.default.arg}" if column.default is not None else ""
                            sql = f"ALTER TABLE {table_name} ADD COLUMN {column.name} {column_type} {nullable} {default}"
                            
                            # Use a connection to execute the SQL
                            with self.engine.connect() as conn:
                                conn.execute(sql)
                else:
                    # Create table if it doesn't exist
                    logger.info(f"Creating table {table_name}")
                    table.create(self.engine)
        else:            
            logger.info("Creating tables")
            Base.metadata.create_all(self.engine)
    
    def get_session(self) -> Session:
        """
        Get a new database session
        
        Returns:
            SQLAlchemy session
        """
        return self.Session()
    
    def close_session(self, session: Session) -> None:
        """
        Close a database session
        
        Args:
            session: SQLAlchemy session to close
        """
        session.close()
    
    def close_all_sessions(self) -> None:
        """Close all sessions"""
        self.Session.remove()
    
    def dispose_engine(self) -> None:
        """Dispose of the engine"""
        self.engine.dispose()


# Singleton instance
db: Database = Database()


def init_db(database_url: str = f"sqlite:///{DEFAULT_DATABASE_NAME}", drop_first: bool = False, recreate_schema: bool = False) -> Database:
    """
    Initialize the database
    
    Args:
        database_url: SQLAlchemy database URL
        drop_first: If True, drop all tables before creating
        recreate_schema: If True, update the schema without dropping data
        
    Returns:
        Database instance
    """
    global db
    db = Database(database_url)
    db.create_tables(drop_first, recreate_schema)
    
    return db 