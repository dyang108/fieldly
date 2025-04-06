from typing import Dict, Any, List, Optional
import json
import re
import logging
import os
import requests
from .extractor import DataExtractor
from constants import DEFAULT_LLM_PROVIDER, PROVIDER_CONFIGS, DEFAULT_TEMPERATURE
from utils.json_utils import extract_json_from_text

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
            use_api: Whether to use an API or local model
            api_key: API key for the provider
            provider: LLM provider name (default from constants)
            model: Model name to use (default from constants)
            api_url: API URL for the provider (default from constants)
            temperature: Temperature for model generation (default from constants)
        """
        # Get provider from argument, environment variable, or default constant
        self.provider = provider or os.environ.get('LLM_PROVIDER') or DEFAULT_LLM_PROVIDER
        self.use_api = use_api
        self.temperature = temperature
        
        # Special case for Ollama - always treat it as a local provider
        if self.provider == "ollama":
            self.use_api = False
        
        # Get provider config
        provider_config = PROVIDER_CONFIGS.get(self.provider, {})
        mode = "api" if self.use_api else "local"
        config = provider_config.get(mode, {})
        
        if self.use_api:
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
        
        logger.info(f"Initialized LLMExtractor with provider={self.provider}, mode={'api' if self.use_api else 'local'}, model={self.model}")
    
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
        
        logger.debug(f"Response text: {response_text}")
        # Parse the response
        if response_text:
            extracted_data = self.clean_json_response(response_text, schema)
            if extracted_data:
                return extracted_data
        
        logger.error("Failed to extract valid JSON from model response")
        return {}
    
    def extract_data_with_context(self, prompt: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from content with contextual information
        
        Args:
            prompt: Prompt for the model that includes instructions for contextual information
            schema: JSON schema defining the structure of the data to extract
            
        Returns:
            Extracted data as a dictionary matching the schema, with metadata
        """
        # Send the prompt to the appropriate model
        if self.use_api:
            response_text = self._call_cloud_api(prompt)
        else:
            response_text = self._call_local_api(prompt)
        
        logger.debug(f"Context response text: {response_text}")
        
        # Parse the response
        if response_text:
            # First, try to parse the full response with metadata
            try:
                # Check if the response is wrapped in triple backticks
                if response_text.strip().startswith('```'):
                    # Extract the JSON content between the backticks
                    json_match = re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', response_text)
                    if json_match:
                        json_str = json_match.group(1)
                        # Clean up common formatting issues
                        json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
                        json_str = re.sub(r',\s*]', ']', json_str)  # Remove trailing commas in arrays
                        json_str = re.sub(r'\s+', ' ', json_str)    # Normalize whitespace
                        data = json.loads(json_str)
                    else:
                        # If we can't extract JSON between backticks, try to parse the whole response
                        data = json.loads(response_text)
                else:
                    # If not wrapped in backticks, try to parse directly
                    data = json.loads(response_text)
                
                # Check if the response has the expected structure with data and metadata
                if isinstance(data, dict) and 'data' in data and 'metadata' in data:
                    # Filter the extracted data to match the schema
                    filtered_data = self.filter_data_by_schema(data['data'], schema)
                    
                    # Add the metadata to the filtered data
                    result = {
                        'data': filtered_data,
                        'metadata': data['metadata']
                    }
                    
                    return result
                # Check for the old format with extracted_data instead of data
                elif isinstance(data, dict) and 'extracted_data' in data and 'metadata' in data:
                    # Filter the extracted data to match the schema
                    filtered_data = self.filter_data_by_schema(data['extracted_data'], schema)
                    
                    # Add the metadata to the filtered data
                    result = {
                        'data': filtered_data,
                        'metadata': data['metadata']
                    }
                    
                    return result
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON response: {str(e)}")
                # If direct parsing fails, try to find JSON in the response
                try:
                    # Look for JSON-like content between triple backticks
                    json_match = re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', response_text)
                    if json_match:
                        json_str = json_match.group(1)
                        # Clean up common formatting issues
                        json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
                        json_str = re.sub(r',\s*]', ']', json_str)  # Remove trailing commas in arrays
                        json_str = re.sub(r'\s+', ' ', json_str)    # Normalize whitespace
                        data = json.loads(json_str)
                        
                        # Check if the response has the expected structure with data and metadata
                        if isinstance(data, dict) and 'data' in data and 'metadata' in data:
                            # Filter the extracted data to match the schema
                            filtered_data = self.filter_data_by_schema(data['data'], schema)
                            
                            # Add the metadata to the filtered data
                            result = {
                                'data': filtered_data,
                                'metadata': data['metadata']
                            }
                            
                            return result
                        # Check for the old format with extracted_data instead of data
                        elif isinstance(data, dict) and 'extracted_data' in data and 'metadata' in data:
                            # Filter the extracted data to match the schema
                            filtered_data = self.filter_data_by_schema(data['extracted_data'], schema)
                            
                            # Add the metadata to the filtered data
                            result = {
                                'data': filtered_data,
                                'metadata': data['metadata']
                            }
                            
                            return result
                except json.JSONDecodeError:
                    pass
            
            # If we couldn't parse the response with metadata, fall back to the standard extraction
            logger.warning("Failed to parse response with metadata, falling back to standard extraction")
            extracted_data = self.clean_json_response(response_text, schema)
            if extracted_data:
                return {
                    'data': extracted_data,
                    'metadata': {}  # Empty metadata
                }
        
        logger.error("Failed to extract valid JSON from model response with context")
        return {
            'data': {},
            'metadata': {}
        }
    
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
                logger.debug(f"Result: {result}")
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
    
    def merge_results(self, chunk_results):
        """
        Merge results from multiple chunks.
        
        Args:
            chunk_results: List of extraction results from different chunks
            
        Returns:
            String explanation of the merge process
        """
        try:
            # If there's only one chunk, no need to merge
            if len(chunk_results) <= 1:
                return "No merging needed - only one chunk processed"
            
            # Extract data fields from all chunks
            all_data = [result.get('data', {}) for result in chunk_results if result and 'data' in result]
            all_metadata = [result.get('metadata', {}) for result in chunk_results if result and 'metadata' in result]
            
            # If any chunk has no data, log it
            if not all(all_data):
                logger.warning(f"Some chunks have no data: {[bool(data) for data in all_data]}")
            
            # Get all unique fields across all chunks
            all_fields = set()
            for data in all_data:
                all_fields.update(data.keys())
            
            # Merge the data - for each field, take the value with highest confidence
            merged_data = {}
            merged_metadata = {}
            merge_explanations = []
            
            for field in all_fields:
                # Collect all values and metadata for this field
                field_values = []
                field_metadata = []
                field_confidences = []
                
                for i, (data, metadata) in enumerate(zip(all_data, all_metadata)):
                    if field in data:
                        field_values.append(data[field])
                        field_metadata.append(metadata.get(field, {}))
                        field_confidences.append(metadata.get(field, {}).get('confidence', 0))
                        
                # Skip if no values found
                if not field_values:
                    continue
                
                # Find the value with highest confidence
                if not field_confidences:
                    # If no confidence scores available, use the first value
                    best_index = 0
                else:
                    best_index = field_confidences.index(max(field_confidences))
                
                merged_data[field] = field_values[best_index]
                merged_metadata[field] = field_metadata[best_index]
                
                # Create an explanation for this field
                if len(field_values) > 1:
                    merge_explanations.append(
                        f"Field '{field}': Selected value from chunk {best_index+1} with "
                        f"confidence {field_confidences[best_index]}"
                    )
            
            # Update the most recent chunk result with merged data
            if chunk_results:
                chunk_results[-1]['data'] = merged_data
                chunk_results[-1]['metadata'] = merged_metadata
            
            # Create a summary explanation of the merge process
            merge_explanation = "Merged data from multiple chunks:\n" + "\n".join(merge_explanations)
            logger.info(f"Merge explanation: {merge_explanation}")
            
            return merge_explanation
        except Exception as e:
            logger.exception(f"Error merging chunk results: {e}")
            return f"Error merging chunk results: {str(e)}"

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
        # Send the prompt to the appropriate model
        if self.use_api:
            response_text = self._call_cloud_api(prompt)
        else:
            response_text = self._call_local_api(prompt)
        
        logger.debug(f"Merge with reasoning response text: {response_text}")
        
        # Parse the response
        if response_text:
            try:
                # First try to parse the full response
                json_match = re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', response_text)
                if json_match:
                    # Extract the JSON content between the backticks
                    json_str = json_match.group(1)
                    # Clean up common formatting issues
                    json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
                    json_str = re.sub(r',\s*]', ']', json_str)  # Remove trailing commas in arrays
                    json_str = re.sub(r'\s+', ' ', json_str)    # Normalize whitespace
                    data = json.loads(json_str)
                else:
                    # If not wrapped in backticks, try to parse directly
                    data = json.loads(response_text)
                
                # Check if the response has the expected structure with merged_data and reasoning
                if isinstance(data, dict) and 'merged_data' in data and 'reasoning' in data:
                    # Filter the merged data to match the schema
                    filtered_data = self.filter_data_by_schema(data['merged_data'], schema)
                    
                    # Return the filtered data and reasoning
                    return {
                        'merged_data': filtered_data,
                        'reasoning': data['reasoning']
                    }
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse JSON response with reasoning: {str(e)}")
                
                # Try to extract JSON using regex
                json_data = extract_json_from_text(response_text)
                if json_data and isinstance(json_data, dict) and 'merged_data' in json_data and 'reasoning' in json_data:
                    # Filter the merged data to match the schema
                    filtered_data = self.filter_data_by_schema(json_data['merged_data'], schema)
                    
                    # Return the filtered data and reasoning
                    return {
                        'merged_data': filtered_data,
                        'reasoning': json_data['reasoning']
                    }
            
            # If we couldn't parse the response with reasoning, fall back to standard merge
            logger.warning("Failed to parse response with reasoning, falling back to standard merge")
            merged_data = self.clean_json_response(response_text, schema)
            if merged_data:
                return {
                    'merged_data': merged_data,
                    'reasoning': {
                        'fallback': 'Could not extract reasoning from model response, using standard merge.'
                    }
                }
        
        logger.error("Failed to extract valid JSON from model merge with reasoning response")
        return {
            'merged_data': {},
            'reasoning': {
                'error': 'Failed to process the model response for merge with reasoning.'
            }
        } 