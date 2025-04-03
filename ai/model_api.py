import re
import json
import logging
import requests
from typing import List, Dict, Any, Optional

from .base import SchemaGenerator

logger = logging.getLogger(__name__)


class APIModelSchemaGenerator(SchemaGenerator):
    """Schema generator using a generic API model"""
    
    def __init__(self, api_key: str, api_url: str, model_name: str):
        """
        Initialize API model generator
        
        Args:
            api_key: API key for authentication
            api_url: API endpoint URL
            model_name: Name of the model to use
        """
        self.api_key = api_key
        self.api_url = api_url
        self.model_name = model_name
    
    def prepare_conversation(self, conversation: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Prepare the conversation for the model
        
        Args:
            conversation: List of conversation messages
            
        Returns:
            List of formatted messages
        """
        # Add system message if not present
        has_system_msg = any(msg.get('role') == 'system' for msg in conversation)
        if not has_system_msg:
            system_msg = {
                "role": "system",
                "content": """You are a JSON schema generator. Your task is to generate a valid JSON schema based on the user's requirements.
IMPORTANT: Your response must be a valid JSON object containing the schema. Do not include any explanations or text outside the JSON.
The response should be in this exact format:
{
  "schema": { ... actual schema ... },
  "suggested_name": "schema_name",
  "message": "Successfully generated schema"
}
Do not include any markdown formatting or code blocks. Just return the raw JSON."""
            }
            return [system_msg] + conversation
        return conversation
    
    def prepare_update_conversation(self, conversation: List[Dict[str, str]], current_schema: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Prepare the conversation for schema updates
        
        Args:
            conversation: List of conversation messages
            current_schema: Current schema to be updated
            
        Returns:
            List of formatted messages
        """
        # Add system message if not present
        has_system_msg = any(msg.get('role') == 'system' for msg in conversation)
        if not has_system_msg:
            system_msg = {
                "role": "system",
                "content": f"""You are a JSON schema updater. Your task is to update the existing schema based on the user's requirements.
Current schema:
{json.dumps(current_schema, indent=2)}

IMPORTANT: Your response must be a valid JSON object containing the updated schema. Do not include any explanations or text outside the JSON.
The response should be in this exact format:
{{
  "schema": {{ ... updated schema ... }},
  "suggested_name": "schema_name",
  "message": "Successfully updated schema"
}}
Do not include any markdown formatting or code blocks. Just return the raw JSON."""
            }
            return [system_msg] + conversation
        return conversation
    
    def _make_api_request(self, messages: List[Dict[str, str]], temperature: float = 0.7) -> Dict[str, Any]:
        """
        Make an API request and handle the response
        
        Args:
            messages: List of conversation messages
            temperature: Temperature parameter for the model
            
        Returns:
            Dict with the API response content
            
        Raises:
            requests.exceptions.RequestException: If the API request fails
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        
        payload = {
            "model": self.model_name,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": 4000
        }
        
        logger.debug(f"Sending request to API: {json.dumps(payload)}")
        response = requests.post(self.api_url, headers=headers, json=payload)
        response.raise_for_status()
        
        result = response.json()
        logger.debug(f"API response: {result}")
        
        # Extract content from the response
        return result["choices"][0]["message"]["content"]
    
    def generate_schema(self, conversation: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Generate a schema using the API model
        
        Args:
            conversation: List of conversation messages
            
        Returns:
            Dict with schema info
        """
        try:
            messages = self.prepare_conversation(conversation)
            content = self._make_api_request(messages)
            return self._parse_response(content)
        except Exception as e:
            logger.error(f"Error generating schema: {str(e)}")
            return {
                "message": f"Error generating schema: {str(e)}",
                "schema": {},
                "suggested_name": "new_schema"
            }
            
    def update_schema(self, conversation: List[Dict[str, str]], current_schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing schema based on conversation
        
        Args:
            conversation: List of conversation messages
            current_schema: The current schema to be updated
            
        Returns:
            Dict with updated schema info
        """
        try:
            messages = self.prepare_update_conversation(conversation, current_schema)
            content = self._make_api_request(messages, temperature=0.5)
            parsed_response = self._parse_response(content)
            
            # If it looks like a schema (has typical schema fields), return it directly
            if (isinstance(parsed_response, dict) and 
                ("$schema" in parsed_response or "type" in parsed_response or "properties" in parsed_response)):
                return current_schema | parsed_response  # Merge with current schema, preferring values from parsed_response
            
            # If we got a wrapped schema with 'schema' field, return it
            if isinstance(parsed_response, dict) and parsed_response.get("schema"):
                return parsed_response
            
            # If parsing failed, return current schema with error message
            return {
                "failed": True,
            }
        except Exception as e:
            logger.error(f"Error updating schema: {str(e)}")
            return {
                "failed": True,
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