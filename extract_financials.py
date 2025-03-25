import json
import logging
from pathlib import Path
import argparse
from typing import Dict, Any, List, Optional
import re
import requests
from requests.exceptions import RequestException

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

OLLAMA_API_HOST = "http://localhost:11434"

def run_ollama_query(text: str, model: str = "deepseek-r1:14b") -> str:
    """Run a query through ollama using the HTTP API."""
    try:
        # Split long prompts if needed
        # if len(text) > 10000:  # Arbitrary threshold, adjust if needed
        #     logger.warning(f"Long prompt detected ({len(text)} chars), truncating to 10000 chars")
        #     text = text[:10000]
        
        # Prepare the request
        url = f"{OLLAMA_API_HOST}/api/generate"
        payload = {
            "model": model,
            "prompt": text,
            "stream": False  # Get complete response
        }
        
        # Make the request
        response = requests.post(url, json=payload, timeout=60)
        response.raise_for_status()
        
        # Parse the response
        result = response.json()
        return result.get('response', '').strip()
        
    except RequestException as e:
        logger.error(f"HTTP error making request to Ollama: {str(e)}")
        return ""
    except Exception as e:
        logger.error(f"Error in run_ollama_query: {str(e)}")
        return ""

def extract_json_from_response(response: str) -> str:
    """Extract JSON object from LLM response, handling common formatting issues."""
    try:
        # Try to find JSON block with a simpler pattern
        # Look for content between outermost braces
        start_idx = response.find('{')
        if start_idx == -1:
            logger.error("No opening brace found in response")
            return ""
        
        # Count braces to find matching closing brace
        count = 0
        in_string = False
        escape_next = False
        
        for i in range(start_idx, len(response)):
            char = response[i]
            
            if escape_next:
                escape_next = False
                continue
                
            if char == '\\':
                escape_next = True
                continue
                
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
                
            if not in_string:
                if char == '{':
                    count += 1
                elif char == '}':
                    count -= 1
                    if count == 0:
                        # Found matching closing brace
                        json_str = response[start_idx:i+1]
                        
                        # Clean up common formatting issues
                        json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas
                        json_str = re.sub(r',\s*]', ']', json_str)  # Remove trailing commas in arrays
                        json_str = re.sub(r'\s+', ' ', json_str)    # Normalize whitespace
                        
                        # Validate JSON
                        json.loads(json_str)  # This will raise JSONDecodeError if invalid
                        return json_str
        
        logger.error("No valid JSON object found in response")
        return ""
        
    except Exception as e:
        logger.error(f"Error extracting JSON: {str(e)}")
        logger.debug(f"Response was: {response}")
        return ""

def create_extraction_prompt(relationships: Dict[str, List[Dict[str, Any]]], page_num: int) -> str:
    """Create a prompt for the LLM to extract financial information from a single page."""
    prompt = f"""Given the following text blocks and their spatial relationships from page {page_num} of a financial report, 
    extract any matching information into a JSON object with the following schema:
    {{
        "companyName": String,
        "reportTitle": String,
        "year": int,
        "address": string,
        "city": string,
        "state": string,
        "zip": string,
        "revenue": int,
        "operatingExpenses": int,
        "dividentsPerShare": float,
        "netIncome": int,
        "totalAssets": int,
        "totalLiabilities": int,
        "cashAndEquivalents": int,
        "risks": string
    }}

    For numerical values, extract only the number (remove currency symbols and commas).
    For text blocks that appear to be risks, concatenate them into a single string.
    If a value is not found on this page, use null.
    Only extract information that appears on this specific page.
    IMPORTANT: Your response must be a valid JSON object matching the schema above.

    Text blocks from page {page_num}:
    """
    
    # Add text blocks and their relationships more concisely
    for text, related_blocks in relationships.items():
        if len(related_blocks) > 0:
            prompt += f"\n'{text}' -> "
            related_texts = [f"'{b['text']}' ({b['alignment']}, dist={b['distance']})" 
                           for b in related_blocks]
            prompt += ", ".join(related_texts)
    
    prompt += "\n\nRespond with ONLY a valid JSON object."
    return prompt

def merge_page_data(pages_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge extracted data from multiple pages, taking the first non-null value for each field."""
    merged = {}
    
    # Define fields and their types
    string_fields = ['companyName', 'reportTitle', 'address', 'city', 'state', 'zip']
    numeric_fields = ['year', 'revenue', 'operatingExpenses', 'netIncome', 
                     'totalAssets', 'totalLiabilities', 'cashAndEquivalents']
    float_fields = ['dividentsPerShare']
    text_fields = ['risks']
    
    # Handle string fields
    for field in string_fields:
        values = [page.get(field) for page in pages_data if page.get(field)]
        merged[field] = values[0] if values else None
    
    # Handle numeric fields
    for field in numeric_fields:
        values = [page.get(field) for page in pages_data if page.get(field) is not None]
        merged[field] = values[0] if values else None
    
    # Handle float fields
    for field in float_fields:
        values = [page.get(field) for page in pages_data if page.get(field) is not None]
        merged[field] = values[0] if values else None
    
    # Concatenate text fields (like risks) from all pages
    for field in text_fields:
        values = [page.get(field) for page in pages_data if page.get(field)]
        merged[field] = ' '.join(values) if values else None
    
    return merged

def clean_llm_response(response: str) -> Dict[str, Any]:
    """Clean and parse the LLM response into a dictionary."""
    try:
        # Extract JSON from response
        json_str = extract_json_from_response(response)
        if not json_str:
            return {}
        
        data = json.loads(json_str)
        
        # Convert numeric strings to numbers where appropriate
        numeric_fields = ['year', 'revenue', 'operatingExpenses', 'dividentsPerShare', 
                         'netIncome', 'totalAssets', 'totalLiabilities', 'cashAndEquivalents']
        
        for field in numeric_fields:
            if isinstance(data.get(field), str):
                # Remove currency symbols, commas, and convert to number
                value = data[field]
                if value:
                    value = re.sub(r'[^\d.-]', '', value)
                    try:
                        if field == 'dividentsPerShare':
                            data[field] = float(value) if value else None
                        else:
                            data[field] = int(float(value)) if value else None
                    except (ValueError, TypeError):
                        data[field] = None
        
        return data
    
    except Exception as e:
        logger.error(f"Error cleaning LLM response: {str(e)}")
        return {}

def process_relationships_file(input_file: Path, output_file: Path, model: str):
    """Process a single relationships file through the LLM."""
    try:
        # Read relationships file
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not data.get('relationships_by_page'):
            logger.warning(f"No relationships found in {input_file}")
            return
        
        pages_data = []
        
        # Process each page separately
        for page_num, page_relationships in data['relationships_by_page'].items():
            logger.info(f"Processing page {page_num}...")
            
            # Create prompt for this page
            prompt = create_extraction_prompt(page_relationships, int(page_num))
            
            # Run through ollama
            logger.info(f"Sending page {page_num} to {model}...")
            response = run_ollama_query(prompt, model)
            
            if not response:
                logger.error(f"No response from LLM for page {page_num}")
                continue
            
            # Clean and parse response
            extracted_data = clean_llm_response(response)
            
            if extracted_data:
                pages_data.append(extracted_data)
            else:
                logger.error(f"Failed to extract structured data from page {page_num}")
        
        if not pages_data:
            logger.error("No data extracted from any page")
            return
        
        # Merge data from all pages
        merged_data = merge_page_data(pages_data)
        
        # Save extracted data
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'filename': data['filename'],
                'total_pages_processed': len(pages_data),
                'extracted_data': merged_data
            }, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Processed {input_file.name} -> {output_file.name}")
        
    except Exception as e:
        logger.error(f"Error processing {input_file}: {str(e)}")

def process_dataset(dataset_name: str, model: str = "deepseek-r1:14b"):
    """Process all relationship files in a dataset."""
    data_dir = Path('.data')
    input_dir = data_dir / f"{dataset_name}-relationships"
    output_dir = data_dir / f"{dataset_name}-extracted"
    
    if not input_dir.exists():
        logger.error(f"Input directory {input_dir} does not exist")
        return
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    relationship_files = list(input_dir.glob('*_relationships.json'))
    
    if not relationship_files:
        logger.warning(f"No relationship files found in {input_dir}")
        return
    
    logger.info(f"Found {len(relationship_files)} relationship files to process")
    
    for input_file in relationship_files:
        output_file = output_dir / input_file.name.replace('_relationships.json', '_extracted.json')
        process_relationships_file(input_file, output_file, model)

def main():
    parser = argparse.ArgumentParser(description='Extract structured data from relationships using LLM')
    parser.add_argument('dataset', help='Name of the dataset directory under .data/')
    parser.add_argument('--model', default='deepseek-r1:14b',
                      help='Ollama model to use (default: deepseek-r1:14b)')
    args = parser.parse_args()
    
    process_dataset(args.dataset, args.model)

if __name__ == '__main__':
    main() 