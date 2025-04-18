import json
import logging
from pathlib import Path
import argparse
from typing import Dict, Any, List, Optional
import re
import requests
from requests.exceptions import RequestException
import os
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from constants import DEFAULT_LOCAL_MODEL, OLLAMA_HOST as DEFAULT_OLLAMA_HOST

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

OLLAMA_API_HOST = DEFAULT_OLLAMA_HOST
DEEPSEEK_API_HOST = "https://api.deepseek.com/v1"

def run_ollama_query(text: str, model: str = DEFAULT_LOCAL_MODEL, use_deepseek: bool = False, api_key: Optional[str] = None) -> str:
    """Run a query through either Ollama or DeepSeek API."""
    try:
        if use_deepseek:
            if not api_key:
                api_key = os.getenv('DEEPSEEK_API_KEY')
                if not api_key:
                    raise ValueError("DeepSeek API key not provided and DEEPSEEK_API_KEY environment variable not set")
            
            # Prepare the request for DeepSeek
            url = f"{DEEPSEEK_API_HOST}/chat/completions"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": model,
                "messages": [{"role": "user", "content": text}],
                "temperature": 0.1,  # Lower temperature for more consistent outputs
                "max_tokens": 4000
            }
            
            # Make the request to DeepSeek
            response = requests.post(url, json=payload, headers=headers, timeout=60)
            response.raise_for_status()
            
            # Parse the response
            result = response.json()
            return result.get('choices', [{}])[0].get('message', {}).get('content', '').strip()
            
        else:
            # Prepare the request for Ollama
            url = f"{OLLAMA_API_HOST}/api/generate"
            payload = {
                "model": model,
                "prompt": text,
                "stream": False
            }
            
            # Make the request to Ollama
            response = requests.post(url, json=payload, timeout=60)
            response.raise_for_status()
            
            # Parse the response
            result = response.json()
            return result.get('response', '').strip()
        
    except RequestException as e:
        logger.error(f"HTTP error making request: {str(e)}")
        return ""
    except Exception as e:
        logger.error(f"Error in run_ollama_query: {str(e)}")
        return ""

def extract_json_from_response(response: str) -> Optional[Dict[str, Any]]:
    """Extract JSON object from LLM response, handling common formatting issues."""
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

def create_extraction_prompt(markdown_content: str, page_num: int) -> str:
    """Create a prompt for the LLM to extract structured data from markdown content."""
    return f"""Extract structured financial data from the following markdown content from page {page_num}.
The response must be a valid JSON object matching this schema:
{{
    "companyName": "string or null",
    "reportTitle": "string or null",
    "reportDate": "string or null",
    "timePeriods": [
        {{
            "period": "string",
            "startDate": "string or null",
            "endDate": "string or null",
            "metrics": {{
                "revenue": "number or null",
                "costOfRevenue": "number or null",
                "grossProfit": "number or null",
                "operatingIncome": "number or null",
                "netIncome": "number or null",
                "eps": "number or null",
                "dilutedEps": "number or null",
                "ebitda": "number or null",
                "operatingCashFlow": "number or null",
                "freeCashFlow": "number or null",
                "capitalExpenditure": "number or null",
                "totalAssets": "number or null",
                "totalLiabilities": "number or null",
                "totalEquity": "number or null",
                "currentAssets": "number or null",
                "currentLiabilities": "number or null",
                "cashAndEquivalents": "number or null",
                "longTermDebt": "number or null",
                "shortTermDebt": "number or null",
                "inventory": "number or null",
                "accountsReceivable": "number or null",
                "accountsPayable": "number or null",
                "depreciation": "number or null",
                "amortization": "number or null",
                "stockBasedCompensation": "number or null",
                "deferredTax": "number or null",
                "deferredRevenue": "number or null",
                "workingCapital": "number or null",
                "operatingMargin": "number or null",
                "profitMargin": "number or null",
                "returnOnEquity": "number or null",
                "returnOnAssets": "number or null",
                "debtToEquity": "number or null",
                "currentRatio": "number or null",
                "quickRatio": "number or null",
                "inventoryTurnover": "number or null",
                "assetTurnover": "number or null",
                "daysSalesOutstanding": "number or null",
                "daysInventoryOutstanding": "number or null",
                "daysPayablesOutstanding": "number or null",
                "operatingCycle": "number or null",
                "cashConversionCycle": "number or null"
            }}
        }}
    ],
    "forwardLookingCapex": [
        {{
            "period": "string",
            "amount": "number",
            "source": {{
                "text": "string",
                "page": "number",
                "context": "string"
            }}
        }}
    ],
    "address": {{
        "street": "string or null",
        "city": "string or null",
        "state": "string or null",
        "zip": "string or null",
        "country": "string or null"
    }},
    "risks": "string or null",
    "notes": "string or null"
}}

Markdown content:
{markdown_content}

Extract all relevant information and return it as a valid JSON object. Include only the fields you can find in the content. Use null for missing fields.
For numeric values, convert all numbers to their numeric form (not strings).
For dates, use ISO format (YYYY-MM-DD).
For the forwardLookingCapex entries, include the exact text where you found the information and its context.
"""

def clean_llm_response(response: str) -> Dict[str, Any]:
    """Clean and validate the LLM's response."""
    try:
        # Extract JSON from response
        data = extract_json_from_response(response)
        if not data:
            logger.error("No valid JSON found in response")
            return {}
        
        # Convert numeric fields from strings to numbers
        def convert_numeric(obj):
            if isinstance(obj, dict):
                return {k: convert_numeric(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numeric(item) for item in obj]
            elif isinstance(obj, str):
                # Try to convert to number if it looks like one
                try:
                    # Remove currency symbols and commas
                    cleaned = obj.replace('$', '').replace(',', '')
                    return float(cleaned)
                except ValueError:
                    return obj
            return obj
        
        return convert_numeric(data)
        
    except Exception as e:
        logger.error(f"Error cleaning LLM response: {str(e)}")
        return {}

def merge_page_data(pages_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Merge extracted data from multiple pages, taking the first non-null value for each field."""
    merged = {
        "companyName": None,
        "reportTitle": None,
        "reportDate": None,
        "timePeriods": [],
        "forwardLookingCapex": [],
        "address": {
            "street": None,
            "city": None,
            "state": None,
            "zip": None,
            "country": None
        },
        "risks": None,
        "notes": None
    }
    
    # Handle basic fields
    for page in pages_data:
        if not merged["companyName"] and page.get("companyName"):
            merged["companyName"] = page["companyName"]
        if not merged["reportTitle"] and page.get("reportTitle"):
            merged["reportTitle"] = page["reportTitle"]
        if not merged["reportDate"] and page.get("reportDate"):
            merged["reportDate"] = page["reportDate"]
        if not merged["risks"] and page.get("risks"):
            merged["risks"] = page["risks"]
        if not merged["notes"] and page.get("notes"):
            merged["notes"] = page["notes"]
        
        # Handle address fields
        if page.get("address"):
            for field in ["street", "city", "state", "zip", "country"]:
                if not merged["address"][field] and page["address"].get(field):
                    merged["address"][field] = page["address"][field]
    
    # Handle time periods
    all_periods = {}
    for page in pages_data:
        if not page.get("timePeriods"):
            continue
            
        for period_data in page["timePeriods"]:
            period = period_data.get("period")
            if not period:
                continue
                
            if period not in all_periods:
                all_periods[period] = {
                    "period": period,
                    "startDate": period_data.get("startDate"),
                    "endDate": period_data.get("endDate"),
                    "metrics": {}
                }
            
            # Merge metrics
            metrics = period_data.get("metrics", {})
            for metric, value in metrics.items():
                if value is not None and metric not in all_periods[period]["metrics"]:
                    all_periods[period]["metrics"][metric] = value
    
    # Convert periods dict to list and sort by period
    merged["timePeriods"] = sorted(
        list(all_periods.values()),
        key=lambda x: x["period"]
    )
    
    # Handle forwardLookingCapex
    all_capex = {}
    for page in pages_data:
        if not page.get("forwardLookingCapex"):
            continue
            
        for capex in page["forwardLookingCapex"]:
            try:
                # Validate required fields
                if not isinstance(capex, dict):
                    logger.warning(f"Invalid capex entry: {capex}")
                    continue
                    
                period = capex.get("period")
                amount = capex.get("amount")
                
                if not period or amount is None:
                    logger.warning(f"Missing required fields in capex entry: {capex}")
                    continue
                
                # Create a unique key for this capex entry
                key = f"{period}_{amount}"
                
                # Ensure source field exists with default values
                if "source" not in capex:
                    capex["source"] = {
                        "text": "",
                        "page": 0,
                        "context": ""
                    }
                elif not isinstance(capex["source"], dict):
                    capex["source"] = {
                        "text": str(capex["source"]),
                        "page": 0,
                        "context": ""
                    }
                
                # Ensure all required source fields exist
                source = capex["source"]
                if "text" not in source:
                    source["text"] = ""
                if "page" not in source:
                    source["page"] = 0
                if "context" not in source:
                    source["context"] = ""
                
                if key not in all_capex:
                    all_capex[key] = capex
                    
            except Exception as e:
                logger.warning(f"Error processing capex entry: {e}")
                continue
    
    # Convert capex dict to list and sort by period
    merged["forwardLookingCapex"] = sorted(
        list(all_capex.values()),
        key=lambda x: x["period"]
    )
    
    return merged

def split_content_into_chunks(content: str, max_chunk_size: int = 4000) -> List[str]:
    """Split markdown content into smaller chunks while preserving structure."""
    chunks = []
    current_chunk = []
    current_size = 0
    
    # Split by paragraphs
    paragraphs = content.split('\n\n')
    
    for para in paragraphs:
        para_size = len(para)
        
        # If adding this paragraph would exceed max size, start a new chunk
        if current_size + para_size > max_chunk_size and current_chunk:
            chunks.append('\n\n'.join(current_chunk))
            current_chunk = []
            current_size = 0
        
        current_chunk.append(para)
        current_size += para_size
    
    # Add the last chunk if it exists
    if current_chunk:
        chunks.append('\n\n'.join(current_chunk))
    
    return chunks

def print_accumulated_data(data: Dict[str, Any], indent: int = 0) -> None:
    """Print accumulated data, excluding null values."""
    indent_str = "  " * indent
    for key, value in data.items():
        if value is None:
            continue
            
        if isinstance(value, dict):
            if any(v is not None for v in value.values()):
                print(f"{indent_str}{key}:")
                print_accumulated_data(value, indent + 1)
        elif isinstance(value, list):
            if value:
                print(f"{indent_str}{key}:")
                for item in value:
                    if isinstance(item, dict):
                        print(f"{indent_str}  -")
                        print_accumulated_data(item, indent + 2)
                    else:
                        print(f"{indent_str}  {item}")
        else:
            print(f"{indent_str}{key}: {value}")

def process_markdown_file(input_file: Path, output_file: Path, model: str, use_deepseek: bool = False, api_key: Optional[str] = None) -> bool:
    """Process a single markdown file through the LLM."""
    try:
        # Skip if output file already exists
        if output_file.exists():
            logger.info(f"Skipping {input_file.name} - output already exists")
            return True
            
        # Read markdown file
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Split content into pages (using markdown horizontal rules)
        pages = content.split('\n\n---\n\n')
        
        pages_data = []
        
        # Process each page separately
        for page_num, page_content in enumerate(pages, 1):
            logger.info(f"Processing page {page_num}...")
            
            # Split page content into smaller chunks
            chunks = split_content_into_chunks(page_content)
            logger.info(f"Split page {page_num} into {len(chunks)} chunks")
            
            page_data = {
                "companyName": None,
                "reportTitle": None,
                "reportDate": None,
                "timePeriods": [],
                "forwardLookingCapex": [],
                "address": {
                    "street": None,
                    "city": None,
                    "state": None,
                    "zip": None,
                    "country": None
                },
                "risks": None,
                "notes": None
            }
            
            # Process each chunk
            for chunk_num, chunk in enumerate(chunks, 1):
                logger.info(f"Processing chunk {chunk_num}/{len(chunks)} of page {page_num}...")
                
                # Create prompt for this chunk
                prompt = create_extraction_prompt(chunk, page_num)
                
                # Run through LLM
                logger.info(f"Sending chunk {chunk_num} to {model}...")
                response = run_ollama_query(prompt, model, use_deepseek, api_key)
                
                if not response:
                    logger.error(f"No response from LLM for chunk {chunk_num}")
                    continue
                
                # Clean and parse response
                chunk_data = clean_llm_response(response)
                
                if chunk_data:
                    # Merge chunk data with page data
                    if chunk_data.get("companyName") and not page_data["companyName"]:
                        page_data["companyName"] = chunk_data["companyName"]
                    if chunk_data.get("reportTitle") and not page_data["reportTitle"]:
                        page_data["reportTitle"] = chunk_data["reportTitle"]
                    if chunk_data.get("reportDate") and not page_data["reportDate"]:
                        page_data["reportDate"] = chunk_data["reportDate"]
                    if chunk_data.get("risks") and not page_data["risks"]:
                        page_data["risks"] = chunk_data["risks"]
                    if chunk_data.get("notes") and not page_data["notes"]:
                        page_data["notes"] = chunk_data["notes"]
                    
                    # Handle address fields
                    if chunk_data.get("address"):
                        for field in ["street", "city", "state", "zip", "country"]:
                            if not page_data["address"][field] and chunk_data["address"].get(field):
                                page_data["address"][field] = chunk_data["address"][field]
                    
                    # Handle time periods
                    if chunk_data.get("timePeriods"):
                        for period_data in chunk_data["timePeriods"]:
                            period = period_data.get("period")
                            if not period:
                                continue
                            
                            # Check if period already exists
                            existing_period = next(
                                (p for p in page_data["timePeriods"] if p["period"] == period),
                                None
                            )
                            
                            if existing_period:
                                # Merge metrics
                                for metric, value in period_data.get("metrics", {}).items():
                                    if value is not None and metric not in existing_period["metrics"]:
                                        existing_period["metrics"][metric] = value
                            else:
                                page_data["timePeriods"].append(period_data)
                    
                    # Handle forwardLookingCapex
                    if chunk_data.get("forwardLookingCapex"):
                        for capex in chunk_data["forwardLookingCapex"]:
                            if "source" not in capex:
                                capex["source"] = {}
                            if "page" not in capex["source"]:
                                capex["source"]["page"] = page_num
                            page_data["forwardLookingCapex"].append(capex)
                    
                    # Print accumulated data after each chunk
                    print(f"\nAccumulated data after chunk {chunk_num}:")
                    print_accumulated_data(page_data)
                    print("\n" + "="*80 + "\n")
                else:
                    logger.error(f"Failed to extract structured data from chunk {chunk_num}")
            
            if any(page_data.values()):  # If we extracted any data
                pages_data.append(page_data)
            else:
                logger.error(f"No data extracted from page {page_num}")
        
        if not pages_data:
            logger.error("No data extracted from any page")
            return False
        
        # Merge data from all pages
        merged_data = merge_page_data(pages_data)
        
        # Save extracted data
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'filename': input_file.name,
                'total_pages_processed': len(pages_data),
                'extracted_data': merged_data
            }, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Processed {input_file.name} -> {output_file.name}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing {input_file}: {str(e)}")
        return False

def process_dataset(dataset_name: str, model: str = DEFAULT_LOCAL_MODEL, use_deepseek: bool = False, api_key: Optional[str] = None) -> None:
    """Process all markdown files in a dataset directory sequentially."""
    data_dir = Path('../.data')
    cached_dir = data_dir / "cached"
    input_dir = cached_dir / f"{dataset_name}-md"
    output_dir = data_dir / f"{dataset_name}-extractedmd"
    
    if not input_dir.exists():
        logger.error(f"Input directory {input_dir} does not exist")
        return
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all markdown files
    md_files = list(input_dir.glob('*.md'))
    
    if not md_files:
        logger.warning(f"No markdown files found in {input_dir}")
        return
    
    logger.info(f"Found {len(md_files)} markdown files to process")
    
    # Process files sequentially
    success_count = 0
    for md_file in md_files:
        output_file = output_dir / md_file.name.replace('.md', '_extracted.json')
        if process_markdown_file(md_file, output_file, model, use_deepseek, api_key):
            success_count += 1
    
    logger.info(f"Processing complete. Successfully processed {success_count}/{len(md_files)} files.")

def main():
    parser = argparse.ArgumentParser(description='Extract structured data from markdown files using LLM')
    parser.add_argument('dataset', help='Name of the dataset directory under .data/')
    parser.add_argument('--model', default=DEFAULT_LOCAL_MODEL,
                      help=f'Model to use (default: {DEFAULT_LOCAL_MODEL})')
    parser.add_argument('--use-deepseek', action='store_true',
                      help='Use DeepSeek API instead of local Ollama')
    parser.add_argument('--api-key', help='DeepSeek API key (can also be set via DEEPSEEK_API_KEY env var)')
    args = parser.parse_args()
    
    process_dataset(args.dataset, args.model, args.use_deepseek, args.api_key)

if __name__ == '__main__':
    main() 