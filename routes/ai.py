import logging
from flask import Blueprint, request, jsonify

from ai import create_schema_generator

logger = logging.getLogger(__name__)

ai_bp = Blueprint('ai', __name__, url_prefix='/api')


@ai_bp.route('/generate-schema', methods=['POST'])
def generate_schema():
    """Generate a JSON schema from a natural language conversation"""
    try:
        conversation = request.json.get('conversation', [])
        
        logger.debug(f"Received schema generation request with conversation: {conversation}")
        
        # Get AI configuration from app config
        ai_type = None
        ai_config = {}
        
        # Check if local model should be used
        use_local_model = request.app.config.get('USE_LOCAL_MODEL', 'false').lower() == 'true'
        
        if use_local_model:
            ai_type = 'deepseek_local'
            ai_config = {
                'model': request.app.config.get('OLLAMA_MODEL', 'deepseek-r1:14b'),
                'api_url': request.app.config.get('OLLAMA_API_URL', 'http://localhost:11434/api/chat')
            }
            logger.info("Using local DeepSeek model through Ollama")
        elif request.app.config.get('DEEPSEEK_API_KEY'):
            ai_type = 'deepseek_api'
            ai_config = {
                'api_key': request.app.config.get('DEEPSEEK_API_KEY'),
                'api_url': request.app.config.get('DEEPSEEK_API_URL', 'https://api.deepseek.com/v1/chat/completions')
            }
            logger.info("Using DeepSeek API")
        else:
            ai_type = 'mock'
            logger.info("Using mock schema generator")
        
        # Create schema generator instance
        schema_generator = create_schema_generator(ai_type, ai_config)
        
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