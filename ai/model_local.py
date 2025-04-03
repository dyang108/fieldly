import re
import json
import logging
import requests
from typing import List, Dict, Any, Optional

from .base import SchemaGenerator
from constants import DEFAULT_LOCAL_MODEL, DEFAULT_OLLAMA_API_URL

logger = logging.getLogger(__name__)


class LocalOllamaSchemaGenerator(SchemaGenerator):
    """Schema generator using a local DeepSeek model via Ollama"""
    
    def __init__(self, model: str = DEFAULT_LOCAL_MODEL, api_url: str = DEFAULT_OLLAMA_API_URL):
        """
        Initialize the local model interface.
        
        Args:
            model: Model name in Ollama (default: DEFAULT_LOCAL_MODEL)
            api_url: URL for the Ollama API (default: DEFAULT_OLLAMA_API_URL)
        """
        self.model = model
        self.api_url = api_url
    
    def generate_schema(self, conversation: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Generate a schema using the local DeepSeek model
        
        Args:
            conversation: List of conversation messages
            
        Returns:
            Dict with schema info
        """
        # Prepare conversation
        messages = self.prepare_conversation(conversation)
        
        try:
            # Convert conversation to Ollama format
            ollama_messages = messages.copy()
            
            payload = {
                "model": self.model,
                "messages": ollama_messages,
                "stream": False
            }
            
            logger.debug(f"Sending request to local Ollama API: {json.dumps(payload)}")
            response = requests.post(self.api_url, json=payload)
            response.raise_for_status()
            
            result = response.json()
            logger.debug(f"Local model response: {result}")
            
            # Extract content from Ollama response
            content = result["message"]["content"]
            
            # Parse the response into a schema
            return self._parse_response(content)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling local model API: {str(e)}")
            return {
                "message": f"Error calling local model API: {str(e)}",
                "schema": {},
                "suggested_name": "new_schema"
            }
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return {
                "message": f"Unexpected error: {str(e)}",
                "schema": {},
                "suggested_name": "new_schema"
            }
    
    def _parse_response(self, content: str) -> Dict[str, Any]:
        """
        Parse the API response into a schema
        
        Args:
            content: API response content
            
        Returns:
            Dict with schema info
        """
        try:
            # First, try to parse the entire response as JSON
            response_data = json.loads(content)
            return response_data
        except json.JSONDecodeError:
            # If that fails, try to extract JSON from markdown code blocks
            try:
                json_match = re.search(r'```json\s*([\s\S]*?)\s*```', content)
                if json_match:
                    json_str = json_match.group(1)
                    response_data = json.loads(json_str)
                    return response_data
                else:
                    # If no code blocks, look for JSON-like structures
                    json_match = re.search(r'({[\s\S]*})', content)
                    if json_match:
                        json_str = json_match.group(1)
                        response_data = json.loads(json_str)
                        return response_data
                    else:
                        # If all else fails, return a basic structure with the raw content
                        return {
                            "message": "Couldn't parse JSON from response",
                            "schema": {},
                            "suggested_name": "new_schema",
                            "raw_response": content
                        }
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON response: {str(e)}")
                return {
                    "message": f"Error parsing schema: {str(e)}",
                    "schema": {},
                    "suggested_name": "new_schema",
                    "raw_response": content if 'content' in locals() else ""
                } 