from abc import ABC, abstractmethod
from typing import List, Dict, Any
import json
import re
import logging

logger = logging.getLogger(__name__)

class SchemaGenerator(ABC):
    """Abstract interface for schema generation models"""
    
    def _clean_json_string(self, json_str: str) -> str:
        """
        Clean a JSON string by removing any non-JSON content and fixing common issues
        
        Args:
            json_str: Raw JSON string that may contain extra content
            
        Returns:
            Cleaned JSON string
        """
        # Remove any content before the first {
        json_str = re.sub(r'^[^{]*', '', json_str)
        # Remove any content after the last }
        json_str = re.sub(r'[^}]*$', '', json_str)
        # Remove any trailing commas
        json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
        return json_str.strip()
    
    def _wrap_schema_response(self, schema: Dict[str, Any], message: str = "Successfully generated schema") -> Dict[str, Any]:
        """
        Wrap a schema in the expected response format
        
        Args:
            schema: The schema to wrap
            message: Optional message to include
            
        Returns:
            Dict with schema wrapped in expected format
        """
        return {
            "schema": schema,
            "suggested_name": schema.get("title", "new_schema"),
            "message": message
        }
    
    def _parse_response(self, content: str) -> Dict[str, Any]:
        """
        Parse the API response into a schema
        
        Args:
            content: API response content
            
        Returns:
            Dict with schema info or the schema itself
        """
        logger.debug(f"Attempting to parse response content: {content[:200]}...")  # Log first 200 chars
        
        try:
            # First, try to parse the entire response as JSON
            logger.debug("Attempting direct JSON parse...")
            parsed = json.loads(content)
            
            # If the response already has the expected format with a 'schema' field, return it
            if isinstance(parsed, dict) and "schema" in parsed:
                logger.debug("Response has expected format with 'schema' field")
                return parsed
            
            # If it looks like a schema directly, return it as is
            if isinstance(parsed, dict) and ("$schema" in parsed or "type" in parsed or "properties" in parsed):
                logger.debug("Response appears to be a schema directly")
                return parsed
            
            # Otherwise, for backward compatibility, wrap the schema in the expected format
            logger.debug("Wrapping parsed result in expected format")
            return self._wrap_schema_response(parsed)
            
        except json.JSONDecodeError as e:
            logger.debug(f"Direct JSON parse failed: {str(e)}")
            
            # If that fails, try to extract JSON from markdown code blocks
            try:
                # Look for JSON in markdown code blocks
                logger.debug("Looking for JSON in markdown code blocks...")
                json_match = re.search(r'```(?:json)?\s*({[\s\S]*?})\s*```', content)
                if json_match:
                    json_str = self._clean_json_string(json_match.group(1))
                    logger.debug(f"Found JSON in code block, cleaned string: {json_str[:200]}...")
                    parsed = json.loads(json_str)
                    
                    # If it looks like a schema directly, return it as is
                    if isinstance(parsed, dict) and ("$schema" in parsed or "type" in parsed or "properties" in parsed):
                        logger.debug("Found schema in code block")
                        return parsed
                    
                    return self._wrap_schema_response(parsed)
                
                # If no code blocks, look for JSON-like structures
                logger.debug("Looking for JSON-like structures...")
                json_match = re.search(r'({[\s\S]*})', content)
                if json_match:
                    json_str = self._clean_json_string(json_match.group(1))
                    logger.debug(f"Found JSON-like structure, cleaned string: {json_str[:200]}...")
                    parsed = json.loads(json_str)
                    
                    # If it looks like a schema directly, return it as is
                    if isinstance(parsed, dict) and ("$schema" in parsed or "type" in parsed or "properties" in parsed):
                        logger.debug("Found schema in JSON-like structure")
                        return parsed
                    
                    return self._wrap_schema_response(parsed)
                
                # If all else fails, return a basic structure with the raw content
                logger.error("Could not find any JSON structure in the response")
                logger.debug(f"Full content that failed to parse: {content}")
                return {
                    "message": "Could not find any JSON structure in the response",
                    "schema": {},
                    "suggested_name": "new_schema",
                    "raw_response": content
                }
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON response: {str(e)}")
                logger.debug(f"Failed JSON string: {json_str if 'json_str' in locals() else 'N/A'}")
                return {
                    "message": f"Error parsing schema: {str(e)}",
                    "schema": {},
                    "suggested_name": "new_schema",
                    "raw_response": content
                }

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
        raise NotImplementedError("Subclasses must implement _make_api_request")

    @abstractmethod
    def generate_schema(self, conversation: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Generate a schema from conversation history
        
        Args:
            conversation: List of conversation messages, each with 'role' and 'content'
                role can be 'user', 'assistant', or 'system'
            
        Returns:
            Dict with schema info including schema, message, suggested_name
        """
        pass

    @abstractmethod
    def update_schema(self, conversation: List[Dict[str, str]], current_schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing schema based on conversation
        
        Args:
            conversation: List of conversation messages, each with 'role' and 'content'
                role can be 'user', 'assistant', or 'system'
            current_schema: The current schema to be updated
            
        Returns:
            Dict with updated schema info
        """
        pass
    
    def prepare_conversation(self, conversation: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """
        Prepare conversation by adding system message if needed
        
        Args:
            conversation: Original conversation history
            
        Returns:
            Prepared conversation history
        """
        messages = conversation.copy()
        
        # Add a system message if not present
        if not any(msg["role"] == "system" for msg in messages):
            messages.insert(0, {
                "role": "system",
                "content": """You are a helpful assistant that generates JSON schemas based on natural language descriptions. 
                When asked to create a schema:
                1. Analyze the user's requirements carefully
                2. Generate a comprehensive JSON schema that captures all the fields mentioned
                3. Include appropriate data types, descriptions, and constraints
                4. Return your response as valid JSON
                5. Structure your response with a 'message' field containing your explanation
                   and a 'schema' field containing the JSON schema object
                6. Include a 'suggested_name' field with a good name for this schema
                """
            })
        
        # If the conversation doesn't end with a specific request for a schema, add it
        if not messages[-1]["content"].lower().strip().endswith("schema"):
            messages.append({
                "role": "user",
                "content": "Based on our conversation, please generate a complete JSON schema."
            })
            
        return messages
        
    def prepare_update_conversation(self, conversation: List[Dict[str, str]], current_schema: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Prepare conversation for schema update
        
        Args:
            conversation: Original conversation history
            current_schema: Current schema to be updated
            
        Returns:
            Prepared conversation history
        """
        messages = conversation.copy()
        
        # Add a system message if not present
        if not any(msg["role"] == "system" for msg in messages):
            messages.insert(0, {
                "role": "system",
                "content": """You are a helpful assistant that updates JSON schemas based on natural language descriptions. 
                When asked to update a schema:
                1. Analyze the user's requirements carefully
                2. Update the existing schema to include the requested changes
                3. Maintain existing fields unless explicitly asked to remove or modify them
                4. Return your response as valid JSON
                5. Structure your response with a 'message' field containing your explanation
                   and a 'schema' field containing the updated JSON schema object
                """
            })
        
        # Ensure we have a final instruction to update the schema
        if not any(msg["content"].lower().find("update") >= 0 and msg["content"].lower().find("schema") >= 0 for msg in messages[-3:]):
            messages.append({
                "role": "user",
                "content": "Based on our conversation, please update the schema accordingly and return the complete updated schema."
            })
            
        return messages 