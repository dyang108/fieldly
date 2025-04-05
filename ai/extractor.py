from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union
import json
import re
import logging
from utils.json_utils import extract_json_from_text

logger = logging.getLogger(__name__)

class DataExtractor(ABC):
    """Abstract interface for data extraction models"""
    
    @abstractmethod
    def extract_data(self, content: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from content according to a schema
        
        Args:
            content: Text content to extract data from
            schema: JSON schema defining the structure of the data to extract
            
        Returns:
            Extracted data as a dictionary matching the schema
        """
        pass
    
    def extract_data_with_context(self, prompt: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from content with contextual information
        
        Args:
            prompt: Prompt for the model that includes instructions for contextual information
            schema: JSON schema defining the structure of the data to extract
            
        Returns:
            Extracted data as a dictionary matching the schema, with metadata
        """
        # This is a default implementation that should be overridden by subclasses
        # The actual implementation will be in LLMExtractor
        logger.warning("extract_data_with_context called on base DataExtractor class, which does not implement it")
        return {}
    
    def create_extraction_prompt(self, content: str, schema: Dict[str, Any]) -> str:
        """
        Create a prompt for data extraction
        
        Args:
            content: Text content to extract data from
            schema: JSON schema defining the structure of the data to extract
            
        Returns:
            Prompt string for the model
        """

        return f"""Please extract structured data from the following content according to this schema, defined in JSON Schema Draft-07 format:

{json.dumps(schema, indent=2)}

Content to extract from:
{content}

Please extract ONLY the relevant information that matches the schema structure. For numeric values:
- Remove currency symbols and commas
- Convert percentages to decimal form (e.g., 25% -> 0.25)
- Use null for missing or unclear values

For dates, use ISO format (YYYY-MM-DD).

Your response must be a valid JSON object matching the schema exactly.
Do not include any explanations or text outside the JSON.
Do NOT include extraneous fields outside of those specified directly in the JSON Schema.
It is okay if the JSON object is empty. It is okay if the JSON object is not complete.

Again, the schema in JSON Schema Draft-07 format is:

{json.dumps(schema, indent=2)}

Response:"""
    
    def filter_data_by_schema(self, data: Any, schema: Dict[str, Any]) -> Any:
        """
        Filter data to only include fields defined in the schema
        
        Args:
            data: Extracted data to filter (any type)
            schema: Schema defining allowed fields and structure
            
        Returns:
            Filtered data containing only schema-defined fields
        """
        # Handle null/None values
        if data is None:
            return None
            
        # Handle primitive types (strings, numbers, booleans)
        if isinstance(data, (str, int, float, bool)):
            return data
            
        # Handle arrays/lists
        if isinstance(data, list):
            # If schema has items definition, apply it to each element
            if isinstance(schema, dict) and 'items' in schema:
                return [self.filter_data_by_schema(item, schema['items']) for item in data]
            # Otherwise, return the list as is
            return data
            
        # Handle objects/dictionaries
        if isinstance(data, dict):
            # Get schema properties (fields that are allowed)
            # If this is a top-level schema with a properties field, use that
            properties = schema.get('properties', {})
            
            # If no properties found but schema is itself a dictionary of field definitions,
            # use the schema directly (for nested objects)
            if not properties and all(not k.startswith('$') for k in schema.keys()):
                properties = schema
                
            # If still no properties, return data as is
            if not properties:
                return data
                
            # Filter the data according to properties
            filtered_data = {}
            for key, value in data.items():
                # If key is in properties, process it
                if key in properties:
                    # Get the schema for this property
                    prop_schema = properties[key]
                    
                    # Apply filtering recursively
                    filtered_value = self.filter_data_by_schema(value, prop_schema)
                    filtered_data[key] = filtered_value
                    
            return filtered_data
            
        # For any other type, return as is
        return data
    
    # TODO: remove union type, only accept dict
    def clean_json_response(self, response: str, schema: Union[Dict[str, Any], str]) -> Optional[Dict[str, Any]]:
        """
        Clean and extract JSON from a model response, filtering to match schema
        
        Args:
            response: Raw response from the model
            schema: Schema to filter the extracted data against (can be dict or JSON string)
            
        Returns:
            Parsed and filtered JSON data or None if parsing fails
        """
        # Ensure schema is a dictionary
        if isinstance(schema, str):
            try:
                # handle the case where the schema is a JSON string wrapped in triple backticks
                # and the case where the schema is just a JSON string
                if re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', schema):
                    schema = json.loads(re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', schema).group(1))
                else:
                    schema = json.loads(schema)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse schema JSON: {str(e)}")
                return None
                
        if not isinstance(schema, dict):
            logger.error(f"Schema must be a dictionary or valid JSON string, got {type(schema)}")
            return None
            
        try:
            # First try direct JSON parsing
            data = json.loads(response)
        except json.JSONDecodeError:
            # If direct parsing fails, try to find JSON in the response
            try:
                # Look for JSON-like content between triple backticks
                json_match = re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', response)
                if json_match:
                    json_str = json_match.group(1)
                    # Clean up common formatting issues
                    json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
                    json_str = re.sub(r',\s*]', ']', json_str)  # Remove trailing commas in arrays
                    json_str = re.sub(r'\s+', ' ', json_str)    # Normalize whitespace
                    data = json.loads(json_str)
                else:
                    # If no code block found, try to find JSON directly
                    json_match = re.search(r'\{[\s\S]*\}', response)
                    if json_match:
                        json_str = json_match.group(0)
                        # Clean up common formatting issues
                        json_str = re.sub(r',\s*}', '}', json_str)
                        json_str = re.sub(r',\s*]', ']', json_str)
                        json_str = re.sub(r'\s+', ' ', json_str)
                        data = json.loads(json_str)
                    else:
                        return None
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON from response: {str(e)}")
                return None
        
        # Filter the data to match the schema structure
        return self.filter_data_by_schema(data, schema)
        
    def merge_results(self, prompt: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge multiple extraction results using the LLM
        
        Args:
            prompt: Prompt for the LLM to merge the results
            schema: Schema defining the structure of the data
            
        Returns:
            Merged data as a dictionary matching the schema
        """
        # This is a default implementation that should be overridden by subclasses
        # The actual implementation will be in LLMExtractor
        logger.warning("merge_results called on base DataExtractor class, which does not implement it")
        return {} 

    def merge_results_with_reasoning(self, prompt: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge multiple extraction results using the LLM and provide reasoning for decisions
        
        Args:
            prompt: Prompt for the LLM to merge the results with reasoning
            schema: Schema defining the structure of the data
            
        Returns:
            Dictionary containing:
                - merged_data: Merged data matching the schema
                - reasoning: Explanations for merge decisions
        """
        # This is a default implementation that should be overridden by subclasses
        # The actual implementation will be in LLMExtractor
        logger.warning("merge_results_with_reasoning called on base DataExtractor class, which does not implement it")
        
        # Return a placeholder structure that matches the expected format
        return {
            "merged_data": {},
            "reasoning": {
                "note": "This is a placeholder. Actual implementation should be in a subclass."
            }
        } 