from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import json
import re
import logging

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
    
    def create_extraction_prompt(self, content: str, schema: Dict[str, Any]) -> str:
        """
        Create a prompt for data extraction
        
        Args:
            content: Text content to extract data from
            schema: JSON schema defining the structure of the data to extract
            
        Returns:
            Prompt string for the model
        """
        return f"""Please extract structured data from the following content according to this schema:

{json.dumps(schema, indent=2)}

Content to extract from:
{content}

Please extract all relevant information that matches the schema structure. For numeric values:
- Remove currency symbols and commas
- Convert percentages to decimal form (e.g., 25% -> 0.25)
- Use null for missing or unclear values

For dates, use ISO format (YYYY-MM-DD).

Your response must be a valid JSON object matching the schema exactly. Do not include any explanations or text outside the JSON.

Response:"""
    
    def clean_json_response(self, response: str) -> Optional[Dict[str, Any]]:
        """
        Clean and extract JSON from a model response
        
        Args:
            response: Raw response from the model
            
        Returns:
            Parsed JSON data or None if parsing fails
        """
        try:
            # First try direct JSON parsing
            return json.loads(response)
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
                    return json.loads(json_str)
                
                # If no code block found, try to find JSON directly
                json_match = re.search(r'\{[\s\S]*\}', response)
                if json_match:
                    json_str = json_match.group(0)
                    # Clean up common formatting issues
                    json_str = re.sub(r',\s*}', '}', json_str)
                    json_str = re.sub(r',\s*]', ']', json_str)
                    json_str = re.sub(r'\s+', ' ', json_str)
                    return json.loads(json_str)
                
                return None
                
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON from response: {str(e)}")
                return None 