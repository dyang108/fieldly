from typing import Dict, Any, List, Optional
import json
import re
import logging
import os
import requests
from .extractor import DataExtractor
from constants import DEFAULT_LLM_PROVIDER, PROVIDER_CONFIGS, DEFAULT_TEMPERATURE

logger = logging.getLogger(__name__)

class LLMExtractor(DataExtractor):
    """
    LLM-based data extractor that can work with different models and providers
    """
    
    def __init__(self, use_api: bool = False, api_key: str = None, 
                 provider: str = None, model: str = None, 
                 api_url: str = None, temperature: float = DEFAULT_TEMPERATURE):
        """
        Initialize the LLM extractor
        
        Args:
            use_api: Whether to use the cloud API (True) or local model (False)
            api_key: API key for the cloud API (required if use_api is True)
            provider: LLM provider name (default from constants)
            model: Model name (overrides the default from constants)
            api_url: API URL (overrides the default from constants)
            temperature: Temperature for model generation (default from constants)
        """
        # Get provider from argument, environment variable, or default constant
        self.provider = provider or os.environ.get('LLM_PROVIDER') or DEFAULT_LLM_PROVIDER
        self.use_api = use_api
        self.temperature = temperature
        
        # Get provider config
        provider_config = PROVIDER_CONFIGS.get(self.provider, {})
        mode = "api" if use_api else "local"
        config = provider_config.get(mode, {})
        
        if use_api:
            # For API mode
            self.api_key = api_key or os.environ.get(f'{self.provider.upper()}_API_KEY')
            if not self.api_key:
                raise ValueError(f"API key is required when using the {self.provider} API")
            
            self.model = model or config.get('model')
            self.api_url = api_url or os.environ.get(f'{self.provider.upper()}_API_URL') or config.get('api_url')
        else:
            # For local mode
            self.model = model or os.environ.get(f'{self.provider.upper()}_LOCAL_MODEL') or config.get('model')
            self.api_url = api_url or os.environ.get(f'{self.provider.upper()}_LOCAL_API_URL') or config.get('api_url')
        
        logger.info(f"Initialized LLMExtractor with provider={self.provider}, mode={'api' if use_api else 'local'}, model={self.model}")
    
    def extract_data(self, content: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from content according to a schema
        
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
        Call the local API with the prompt
        
        Args:
            prompt: Prompt to send to the model
            
        Returns:
            Model response text or None if the call fails
        """
        try:
            # Handle provider-specific local API calls
            if self.provider == "ollama" or self.provider == "deepseek":
                # Prepare the payload for Ollama/local DeepSeek
                payload = {
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False
                }
                
                logger.debug(f"Sending request to local {self.provider} API: {self.api_url}")
                response = requests.post(self.api_url, json=payload)
                response.raise_for_status()
                
                result = response.json()
                logger.debug(f"Received response from local {self.provider} API")
                
                # Extract content from response
                return result["message"]["content"]
            else:
                logger.error(f"Local API not supported for provider: {self.provider}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling local API: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in local API call: {str(e)}")
            return None
    
    def _call_cloud_api(self, prompt: str) -> Optional[str]:
        """
        Call the cloud API with the prompt
        
        Args:
            prompt: Prompt to send to the model
            
        Returns:
            Model response text or None if the call fails
        """
        try:
            headers = {
                "Content-Type": "application/json"
            }
            
            # Handle provider-specific API calls
            if self.provider == "deepseek":
                headers["Authorization"] = f"Bearer {self.api_key}"
                
                payload = {
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": self.temperature,
                    "max_tokens": 4000
                }
                
                logger.debug(f"Sending request to DeepSeek cloud API: {self.api_url}")
                response = requests.post(self.api_url, headers=headers, json=payload)
                response.raise_for_status()
                
                result = response.json()
                logger.debug("Received response from DeepSeek cloud API")
                
                # Extract content from the cloud API response
                return result["choices"][0]["message"]["content"]
                
            elif self.provider == "openai":
                headers["Authorization"] = f"Bearer {self.api_key}"
                
                payload = {
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": self.temperature,
                    "max_tokens": 4000
                }
                
                logger.debug(f"Sending request to OpenAI API: {self.api_url}")
                response = requests.post(self.api_url, headers=headers, json=payload)
                response.raise_for_status()
                
                result = response.json()
                logger.debug("Received response from OpenAI API")
                
                # Extract content from the OpenAI API response
                return result["choices"][0]["message"]["content"]
                
            elif self.provider == "anthropic":
                headers["x-api-key"] = self.api_key
                headers["anthropic-version"] = "2023-06-01"
                
                payload = {
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": self.temperature,
                    "max_tokens": 4000
                }
                
                logger.debug(f"Sending request to Anthropic API: {self.api_url}")
                response = requests.post(self.api_url, headers=headers, json=payload)
                response.raise_for_status()
                
                result = response.json()
                logger.debug("Received response from Anthropic API")
                
                # Extract content from the Anthropic API response
                return result["content"][0]["text"]
                
            else:
                logger.error(f"API provider not supported: {self.provider}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error calling {self.provider} API: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in {self.provider} API call: {str(e)}")
            return None 