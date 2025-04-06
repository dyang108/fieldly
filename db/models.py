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
    """
    Model for tracking extraction progress
    """
    __tablename__ = 'extraction_progress'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dataset_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    total_files: Mapped[int] = mapped_column(Integer, nullable=False)
    processed_files: Mapped[int] = mapped_column(Integer, nullable=False)
    current_file: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    current_file_index: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Current file index in the files array
    file_progress: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    current_file_chunks: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    current_file_chunk: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    total_chunks: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    current_chunk: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # Current chunk being processed
    processed_chunks: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    files: Mapped[Optional[str]] = mapped_column(Text, nullable=True) # JSON list of files
    merged_data: Mapped[Optional[str]] = mapped_column(Text, nullable=True) # JSON for merged data
    merge_reasoning_history: Mapped[Optional[str]] = mapped_column(Text, nullable=True) # JSON for merge reasoning history
    schema: Mapped[Optional[str]] = mapped_column(Text, nullable=True) # JSON schema for extraction
    start_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, default=datetime.now)
    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now)
    duration: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    
    def __repr__(self):
        return f"<ExtractionProgress(id={self.id}, dataset={self.dataset_name}, status={self.status})>"
    
    def to_dict(self):
        result = {
            'id': self.id,
            'dataset_name': self.dataset_name,
            'source': self.source,
            'status': self.status,
            'message': self.message,
            'total_files': self.total_files,
            'processed_files': self.processed_files,
            'current_file': self.current_file,
            'current_file_index': self.current_file_index,
            'file_progress': self.file_progress,
            'current_file_chunks': self.current_file_chunks,
            'current_file_chunk': self.current_file_chunk,
            'total_chunks': self.total_chunks,
            'current_chunk': self.current_chunk,
            'processed_chunks': self.processed_chunks
        }
        
        # Add timestamps if available
        if self.start_time:
            result['start_time'] = self.start_time.isoformat()
        if self.end_time:
            result['end_time'] = self.end_time.isoformat()
        if self.updated_at:
            result['updated_at'] = self.updated_at.isoformat()
        if self.duration is not None:
            result['duration'] = self.duration
            
        # Parse JSON fields
        try:
            if self.files:
                result['files'] = json.loads(self.files)
        except:
            result['files'] = []
            
        try:
            if self.merged_data:
                result['merged_data'] = json.loads(self.merged_data)
        except:
            result['merged_data'] = {}
            
        try:
            if self.merge_reasoning_history:
                result['merge_reasoning_history'] = json.loads(self.merge_reasoning_history)
        except:
            result['merge_reasoning_history'] = []
            
        try:
            if self.schema:
                result['schema'] = json.loads(self.schema)
        except:
            result['schema'] = None
            
        return result
    
    def set_files(self, files_list):
        """Set the files list as JSON"""
        self.files = json.dumps(files_list)
        
    def set_merged_data(self, data):
        """Set the merged data as JSON"""
        self.merged_data = json.dumps(data)
        
    def set_merge_reasoning_history(self, history):
        """Set the merge reasoning history as JSON"""
        self.merge_reasoning_history = json.dumps(history)
        
    def set_merged_data_with_reasoning(self, merged_data, reasoning_entry):
        """Update both merged data and add to reasoning history"""
        # Update merged data
        self.set_merged_data(merged_data)
        
        # Add reasoning to history
        current_history = []
        try:
            if self.merge_reasoning_history:
                current_history = json.loads(self.merge_reasoning_history)
        except:
            current_history = []
            
        current_history.append(reasoning_entry)
        self.set_merge_reasoning_history(current_history) 