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
    
    def create_tables(self, drop_first: bool = False) -> None:
        """
        Create all tables defined in models
        
        Args:
            drop_first: If True, drop all tables before creating
        """
        if drop_first:
            logger.warning("Dropping all tables")
            Base.metadata.drop_all(self.engine)
            
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


def init_db(database_url: str = f"sqlite:///{DEFAULT_DATABASE_NAME}", drop_first: bool = False) -> Database:
    """
    Initialize the database
    
    Args:
        database_url: SQLAlchemy database URL
        drop_first: If True, drop all tables before creating
        
    Returns:
        Database instance
    """
    global db
    db = Database(database_url)
    db.create_tables(drop_first)
    
    return db 