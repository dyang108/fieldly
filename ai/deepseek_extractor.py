from typing import Dict, Any, List, Optional
import json
import re
import logging
import requests
from .extractor import DataExtractor

logger = logging.getLogger(__name__)

class DeepSeekExtractor(DataExtractor):
    """DeepSeek implementation of data extraction"""
    
    def __init__(self, use_api: bool = False, api_key: str = None, 
                 model: str = "deepseek-r1:14b", 
                 api_url: str = "http://localhost:11434/api/chat",
                 cloud_api_url: str = "https://api.deepseek.com/v1/chat/completions"):
        """
        Initialize the DeepSeek extractor
        
        Args:
            use_api: Whether to use the cloud API (True) or local model via Ollama (False)
            api_key: API key for the cloud API (required if use_api is True)
            model: Model name for local Ollama (default: deepseek-r1:14b)
            api_url: URL for local Ollama API (default: http://localhost:11434/api/chat)
            cloud_api_url: URL for DeepSeek cloud API (default: https://api.deepseek.com/v1/chat/completions)
        """
        self.use_api = use_api
        
        if use_api:
            if not api_key:
                raise ValueError("API key is required when using the cloud API")
            self.api_key = api_key
            self.api_url = cloud_api_url
        else:
            self.model = model
            self.api_url = api_url
    
    def extract_data(self, content: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from content according to a schema using DeepSeek
        
        Args:
            content: Text content to extract data from
            schema: JSON schema defining the structure of the data to extract
            
        Returns:
            Extracted data as a dictionary matching the schema
        """
        # Create the prompt for extraction
        prompt = self.create_extraction_prompt(content, schema)
        
        # Send the prompt to the appropriate model
        if self.use_api:
            response_text = self._call_cloud_api(prompt)
        else:
            response_text = self._call_local_api(prompt)
        
        # Parse the response
        if response_text:
            extracted_data = self.clean_json_response(response_text, schema)
            if extracted_data:
                return extracted_data
        
        logger.error("Failed to extract valid JSON from model response")
        return {}
    
    def _call_local_api(self, prompt: str) -> Optional[str]:
        """
        Call the local Ollama API with the prompt
        
        Args:
            prompt: Prompt to send to the model
            
        Returns:
            Model response text or None if the call fails
        """
        try:
            # Prepare the payload for Ollama
            payload = {
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False
            }
            
            logger.debug(f"Sending request to local Ollama API: {self.api_url}")
            response = requests.post(self.api_url, json=payload)
            response.raise_for_status()
            
            result = response.json()
            logger.debug("Received response from local Ollama API")
            
            # Extract content from Ollama response
            return result["message"]["content"]
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling local Ollama API: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in local API call: {str(e)}")
            return None
    
    def _call_cloud_api(self, prompt: str) -> Optional[str]:
        """
        Call the DeepSeek cloud API with the prompt
        
        Args:
            prompt: Prompt to send to the model
            
        Returns:
            Model response text or None if the call fails
        """
        try:
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            payload = {
                "model": "deepseek-chat",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,  # Lower temperature for more deterministic extraction
                "max_tokens": 4000
            }
            
            logger.debug(f"Sending request to DeepSeek cloud API: {self.api_url}")
            response = requests.post(self.api_url, headers=headers, json=payload)
            response.raise_for_status()
            
            result = response.json()
            logger.debug("Received response from DeepSeek cloud API")
            
            # Extract content from the cloud API response
            return result["choices"][0]["message"]["content"]
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling DeepSeek cloud API: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in cloud API call: {str(e)}")
            return None 