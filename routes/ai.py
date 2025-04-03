import logging
import json
from flask import Blueprint, request, jsonify
from typing import Dict, Any, List, Optional, cast

from db import db, Schema
from ai import create_schema_generator
from constants import DEFAULT_LOCAL_MODEL, DEFAULT_OLLAMA_API_URL
import os

logger = logging.getLogger(__name__)

ai_bp = Blueprint('ai', __name__, url_prefix='/api')


@ai_bp.route('/generate-schema', methods=['POST'])
def generate_schema():
    """Generate a JSON schema from a natural language conversation"""
    try:
        conversation = request.json.get('conversation', [])
        
        logger.debug(f"Received schema generation request with conversation: {conversation}")
        
        # Get AI configuration from app config
        use_local_model = os.getenv('USE_LOCAL_MODEL', 'true').lower() == 'true'
        
        if use_local_model:
            # Use local model configuration
            schema_generator = create_schema_generator(
                use_local_model=True,
                model=os.getenv('OLLAMA_MODEL', DEFAULT_LOCAL_MODEL),
                api_url=os.getenv('OLLAMA_API_URL', DEFAULT_OLLAMA_API_URL)
            )
            logger.info("Using local model through Ollama")
        elif os.getenv('DEEPSEEK_API_KEY'):
            # Use API configuration
            schema_generator = create_schema_generator(
                use_local_model=False,
                api_key=os.getenv('DEEPSEEK_API_KEY'),
                model=os.getenv('DEEPSEEK_MODEL', 'deepseek-chat'),
                api_url=os.getenv('DEEPSEEK_API_URL', 'https://api.deepseek.com/v1/chat/completions')
            )
            logger.info("Using API model")
        else:
            # Use mock generator
            schema_generator = MockSchemaGenerator()
            logger.info("Using mock schema generator")
        
        # Generate schema
        result = schema_generator.generate_schema(conversation)
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error generating schema: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'message': 'Error generating schema',
            'schema': {},
            'suggested_name': 'error_schema'
        }), 500


@ai_bp.route('/edit-schema', methods=['POST'])
def edit_schema():
    """Edit an existing JSON schema using a natural language conversation"""
    try:
        conversation = request.json.get('conversation', [])
        schema_id = request.json.get('schema_id')
        
        if not schema_id:
            return jsonify({'error': 'schema_id is required'}), 400
            
        session = db.get_session()
        schema = session.query(Schema).get(schema_id)
        if not schema:
            return jsonify({'error': 'Schema not found'}), 404
            
        current_schema = schema.get_schema()
        
        # Get AI configuration from app config
        use_local_model = os.getenv('USE_LOCAL_MODEL', 'true').lower() == 'true'
        
        if use_local_model:
            # Use local model configuration
            schema_generator = create_schema_generator(
                use_local_model=True,
                model=os.getenv('OLLAMA_MODEL', DEFAULT_LOCAL_MODEL),
                api_url=os.getenv('OLLAMA_API_URL', DEFAULT_OLLAMA_API_URL)
            )
            logger.info("Using local model through Ollama")
        elif os.getenv('DEEPSEEK_API_KEY'):
            # Use API configuration
            schema_generator = create_schema_generator(
                use_local_model=False,
                api_key=os.getenv('DEEPSEEK_API_KEY'),
                model=os.getenv('DEEPSEEK_MODEL', 'deepseek-chat'),
                api_url=os.getenv('DEEPSEEK_API_URL', 'https://api.deepseek.com/v1/chat/completions')
            )
            logger.info("Using API model")
        else:
            # Use mock generator
            schema_generator = MockSchemaGenerator()
            logger.info("Using mock schema generator")
        
        # Prepare conversation with context
        full_conversation = []
        
        # Add system message with context if not present
        has_system_msg = any(msg.get('role') == 'system' for msg in conversation)
        if not has_system_msg:
            full_conversation.append({
                'role': 'system',
                'content': f'You are editing a schema named "{schema.name}". The current schema is provided as context.'
            })
        
        # Add context message with current schema
        full_conversation.append({
            'role': 'system',
            'content': f'Current schema: {json.dumps(current_schema)}'
        })
        
        # Add user conversation
        full_conversation.extend(conversation)
        
        # Generate updated schema
        result = schema_generator.update_schema(full_conversation, current_schema)
        
        return jsonify({
            'message': result.get('message', 'Schema updated successfully'),
            'updated_schema': result.get('schema', current_schema)
        })
    except Exception as e:
        logger.error(f"Error editing schema: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'message': 'Error editing schema',
            'updated_schema': current_schema
        }), 500
    finally:
        db.close_session(session) 