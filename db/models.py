import json
from datetime import datetime
from typing import Dict, Any, List, Optional, cast
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
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