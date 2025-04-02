import logging
from flask import Blueprint, request, jsonify
from typing import Dict, Any

from db import db, Schema

logger = logging.getLogger(__name__)

schemas_bp = Blueprint('schemas', __name__, url_prefix='/api/schemas')


@schemas_bp.route('', methods=['GET'])
def get_schemas():
    """Get all schemas"""
    session = db.get_session()
    try:
        logger.info("Starting GET /api/schemas request")
        schemas = session.query(Schema).all()
        logger.info(f"Successfully retrieved {len(schemas)} schemas from database")
        
        result = []
        for schema in schemas:
            try:
                schema_dict = {
                    'id': schema.id,
                    'name': schema.name,
                    'schema': schema.get_schema(),
                    'created_at': schema.created_at.isoformat() if schema.created_at else None
                }
                logger.debug(f"Processed schema: {schema.name} (ID: {schema.id})")
                result.append(schema_dict)
            except Exception as e:
                logger.error(f"Error processing schema {schema.id}: {str(e)}")
                continue
        
        logger.info("Successfully prepared schema response")
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in GET /api/schemas: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)


@schemas_bp.route('', methods=['POST'])
def create_schema():
    """Create a new schema"""
    session = db.get_session()
    try:
        logger.info("Starting POST /api/schemas request")
        data = request.get_json()
        logger.debug(f"Received data: {data}")
        
        if not data or 'name' not in data or 'schema' not in data:
            logger.error("Missing required fields in request data")
            return jsonify({'error': 'Missing required fields'}), 400
            
        schema = Schema(
            name=data['name']
        )
        schema.set_schema(data['schema'])
        logger.info(f"Created new schema object: {schema.name}")
        
        session.add(schema)
        logger.debug("Added schema to database session")
        
        try:
            session.commit()
            logger.info(f"Successfully committed schema {schema.id} to database")
            return jsonify({
                'id': schema.id,
                'name': schema.name,
                'schema': schema.get_schema(),
                'created_at': schema.created_at.isoformat() if schema.created_at else None
            }), 201
        except Exception as commit_error:
            logger.error(f"Database commit error: {str(commit_error)}", exc_info=True)
            session.rollback()
            return jsonify({'error': 'Database error'}), 500
            
    except Exception as e:
        logger.error(f"Error in POST /api/schemas: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)


@schemas_bp.route('/<int:id>', methods=['GET'])
def get_schema(id):
    """Get a schema by ID"""
    session = db.get_session()
    try:
        schema = session.query(Schema).get(id)
        
        if not schema:
            return jsonify({'error': 'Schema not found'}), 404
        
        return jsonify({
            'id': schema.id,
            'name': schema.name,
            'schema': schema.get_schema(),
            'created_at': schema.created_at.isoformat() if schema.created_at else None
        })
    except Exception as e:
        logger.error(f"Error in GET /api/schemas/{id}: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)


@schemas_bp.route('/<int:id>', methods=['PUT'])
def update_schema(id):
    """Update a schema by ID"""
    session = db.get_session()
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        schema = session.query(Schema).get(id)
        if not schema:
            return jsonify({'error': 'Schema not found'}), 404
        
        if 'schema' in data:
            schema.set_schema(data['schema'])
            
        if 'name' in data:
            schema.name = data['name']
        
        session.commit()
        
        return jsonify({
            'id': schema.id,
            'name': schema.name,
            'schema': schema.get_schema(),
            'created_at': schema.created_at.isoformat() if schema.created_at else None
        })
    except Exception as e:
        session.rollback()
        logger.error(f"Error in PUT /api/schemas/{id}: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session)


@schemas_bp.route('/<int:id>', methods=['DELETE'])
def delete_schema(id):
    """Delete a schema by ID"""
    session = db.get_session()
    try:
        schema = session.query(Schema).get(id)
        if not schema:
            return jsonify({'error': 'Schema not found'}), 404
        
        session.delete(schema)
        session.commit()
        
        return jsonify({'message': 'Schema deleted successfully'})
    except Exception as e:
        session.rollback()
        logger.error(f"Error in DELETE /api/schemas/{id}: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        db.close_session(session) 