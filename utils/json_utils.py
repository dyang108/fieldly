"""Utility functions for JSON handling."""
import re
import json
from typing import Optional, Dict, Any


def clean_json_string(json_str: str) -> str:
    """
    Clean up common formatting issues in JSON strings.
    
    Args:
        json_str: The JSON string to clean
        
    Returns:
        Cleaned JSON string
    """
    # Remove trailing commas
    json_str = re.sub(r',\s*}', '}', json_str)
    json_str = re.sub(r',\s*]', ']', json_str)
    # Normalize whitespace
    json_str = re.sub(r'\s+', ' ', json_str)
    return json_str


def extract_json_from_text(text: str) -> Optional[Dict[str, Any]]:
    """
    Extract and parse JSON from text that might contain markdown or other formatting.
    
    Args:
        text: Text that might contain JSON
        
    Returns:
        Parsed JSON data or None if no valid JSON found
    """
    try:
        # First try direct parsing
        return json.loads(text)
    except json.JSONDecodeError:
        # Look for JSON-like content between triple backticks
        json_match = re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', text)
        if json_match:
            json_str = json_match.group(1)
            # Clean up common formatting issues
            json_str = clean_json_string(json_str)
            return json.loads(json_str)
        else:
            # If no code block found, try to find JSON directly
            json_match = re.search(r'\{[\s\S]*\}', text)
            if json_match:
                json_str = json_match.group(0)
                # Clean up common formatting issues
                json_str = clean_json_string(json_str)
                return json.loads(json_str)
    
    return None 