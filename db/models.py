import json
from datetime import datetime
from typing import Dict, Any, List, Optional, cast
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Float, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Mapped, mapped_column

from type_definitions import SchemaDefinition, StorageType

Base = declarative_base()


class Schema(Base):
    """Schema model for storing JSON schemas"""
    
    __tablename__ = 'schemas'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    schema: Mapped[str] = mapped_column(String, nullable=False)  # JSON stored as string
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # Relationships
    mappings: Mapped[List["DatasetSchemaMapping"]] = relationship("DatasetSchemaMapping", back_populates="schema")
    
    def get_schema(self) -> SchemaDefinition:
        """Get the schema as a Python object"""
        return cast(SchemaDefinition, json.loads(self.schema) if self.schema else {})
    
    def set_schema(self, schema_data: SchemaDefinition) -> None:
        """Set the schema from a Python object"""
        self.schema = json.dumps(schema_data)
    
    def __repr__(self) -> str:
        return f"<Schema(id={self.id}, name='{self.name}')>"


class DatasetSchemaMapping(Base):
    """Model for mapping datasets to schemas"""
    
    __tablename__ = 'dataset_schema_mappings'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset_name: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)  # 'local' or 's3'
    schema_id: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('schemas.id'), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # Relationships
    schema: Mapped[Optional[Schema]] = relationship("Schema", back_populates="mappings")
    
    def __repr__(self) -> str:
        return f"<DatasetSchemaMapping(id={self.id}, dataset='{self.dataset_name}', source='{self.source}')>"


class ExtractionProgress(Base):
    """Model for tracking extraction progress"""
    
    __tablename__ = 'extraction_progress'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    
    # Identification 
    dataset_name: Mapped[str] = mapped_column(String, nullable=False)
    source: Mapped[str] = mapped_column(String, nullable=False)  # 'local' or 's3'
    
    # Status information
    status: Mapped[str] = mapped_column(String, nullable=False, default='in_progress')  # 'in_progress', 'completed', 'failed', 'interrupted'
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # For error messages or completion info
    
    # Files information
    total_files: Mapped[int] = mapped_column(Integer, default=0)
    processed_files: Mapped[int] = mapped_column(Integer, default=0)
    current_file: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    file_progress: Mapped[float] = mapped_column(Float, default=0.0)  # 0.0 to 1.0
    
    # Chunk information
    total_chunks: Mapped[int] = mapped_column(Integer, default=0)
    processed_chunks: Mapped[int] = mapped_column(Integer, default=0)
    current_file_chunks: Mapped[int] = mapped_column(Integer, default=0)
    current_file_chunk: Mapped[int] = mapped_column(Integer, default=0)
    
    # Data storage
    files: Mapped[str] = mapped_column(Text, default='[]')  # JSON list of files
    merged_data: Mapped[str] = mapped_column(Text, default='{}')  # JSON object
    merge_reasoning_history: Mapped[str] = mapped_column(Text, default='[]')  # JSON array of reasoning entries
    
    # Timestamps
    start_time: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    duration: Mapped[Optional[float]] = mapped_column(Float, nullable=True)  # In seconds
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def get_files(self) -> List[str]:
        """Get the list of files from the JSON string"""
        return json.loads(self.files if self.files else '[]')
    
    def set_files(self, files_list: List[str]) -> None:
        """Set the files list"""
        self.files = json.dumps(files_list)
    
    def get_merged_data(self) -> Dict[str, Any]:
        """Get the merged data as a Python dictionary"""
        return json.loads(self.merged_data if self.merged_data else '{}')
    
    def set_merged_data(self, data: Dict[str, Any]) -> None:
        """Set the merged data"""
        self.merged_data = json.dumps(data)
    
    def get_merge_reasoning_history(self) -> List[Dict[str, Any]]:
        """Get merge reasoning history as a Python list"""
        return json.loads(self.merge_reasoning_history if self.merge_reasoning_history else '[]')
    
    def set_merge_reasoning_history(self, history: List[Dict[str, Any]]) -> None:
        """Set the merge reasoning history"""
        self.merge_reasoning_history = json.dumps(history)
    
    def add_merge_reasoning(self, reasoning: Dict[str, Any]) -> None:
        """Add a new reasoning entry to the history"""
        history = self.get_merge_reasoning_history()
        history.append(reasoning)
        self.set_merge_reasoning_history(history)
    
    def set_merged_data_with_reasoning(self, merged_data: Dict[str, Any], reasoning_entry: Dict[str, Any]) -> None:
        """
        Update the merged data and add a reasoning entry to the history
        
        Args:
            merged_data: The updated merged data
            reasoning_entry: Information about the reasoning behind merge decisions
        """
        self.set_merged_data(merged_data)
        self.add_merge_reasoning(reasoning_entry)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the model to a dictionary for API responses"""
        return {
            'id': self.id,
            'dataset_name': self.dataset_name,
            'source': self.source,
            'status': self.status,
            'message': self.message,
            'total_files': self.total_files,
            'processed_files': self.processed_files,
            'current_file': self.current_file,
            'file_progress': self.file_progress,
            'total_chunks': self.total_chunks,
            'processed_chunks': self.processed_chunks,
            'current_file_chunks': self.current_file_chunks,
            'current_file_chunk': self.current_file_chunk,
            'files': self.get_files(),
            'merged_data': self.get_merged_data(),
            'merge_reasoning_history': self.get_merge_reasoning_history(),
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration': self.duration,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    def __repr__(self) -> str:
        return f"<ExtractionProgress(id={self.id}, dataset='{self.dataset_name}', source='{self.source}', status='{self.status}')>" 