import re
import json
import logging
import requests
from typing import List, Dict, Any, Optional

from .base import SchemaGenerator

logger = logging.getLogger(__name__)


class APIModelSchemaGenerator(SchemaGenerator):
    """Schema generator using the DeepSeek API"""
    
    def __init__(self, api_key: str, api_url: str = "https://api.deepseek.com/v1/chat/completions"):
        """
        Initialize DeepSeek API generator
        
        Args:
            api_key: DeepSeek API key
            api_url: DeepSeek API URL (default: https://api.deepseek.com/v1/chat/completions)
        """
        self.api_key = api_key
        self.api_url = api_url
    
    def generate_schema(self, conversation: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Generate a schema using the DeepSeek API
        
        Args:
            conversation: List of conversation messages
            
        Returns:
            Dict with schema info
        """
        # Prepare conversation
        messages = self.prepare_conversation(conversation)
        
        try:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            payload = {
                "model": "deepseek-chat",
                "messages": messages,
                "temperature": 0.7,
                "max_tokens": 4000
            }
            
            logger.debug(f"Sending request to DeepSeek API: {json.dumps(payload)}")
            response = requests.post(self.api_url, headers=headers, json=payload)
            response.raise_for_status()
            
            result = response.json()
            logger.debug(f"DeepSeek API response: {result}")
            
            # Extract content from the response
            content = result["choices"][0]["message"]["content"]
            
            # Parse the response into a schema
            return self._parse_response(content)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling DeepSeek API: {str(e)}")
            return {
                "message": f"Error calling DeepSeek API: {str(e)}",
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
                    "raw_response": content
                } 