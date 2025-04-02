import json
from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Schema(Base):
    """Schema model for storing JSON schemas"""
    
    __tablename__ = 'schemas'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    schema = Column(String, nullable=False)  # JSON stored as string
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    mappings = relationship("DatasetSchemaMapping", back_populates="schema")
    
    def get_schema(self):
        """Get the schema as a Python object"""
        return json.loads(self.schema) if self.schema else {}
    
    def set_schema(self, schema_data):
        """Set the schema from a Python object"""
        self.schema = json.dumps(schema_data)
    
    def __repr__(self):
        return f"<Schema(id={self.id}, name='{self.name}')>"


class DatasetSchemaMapping(Base):
    """Model for mapping datasets to schemas"""
    
    __tablename__ = 'dataset_schema_mappings'
    
    id = Column(Integer, primary_key=True)
    dataset_name = Column(String, nullable=False)
    source = Column(String, nullable=False)  # 'local' or 's3'
    schema_id = Column(Integer, ForeignKey('schemas.id'), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    schema = relationship("Schema", back_populates="mappings")
    
    def __repr__(self):
        return f"<DatasetSchemaMapping(id={self.id}, dataset='{self.dataset_name}', source='{self.source}')>" 