import json
import logging
from pathlib import Path
import argparse
from typing import Dict, Any, List, Optional
import re
import requests
from requests.exceptions import RequestException
import os

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

OLLAMA_API_HOST = "http://localhost:11434"
DEEPSEEK_API_HOST = "https://api.deepseek.com/v1"

def run_ollama_query(text: str, model: str = "deepseek-r1:14b", use_deepseek: bool = False, api_key: Optional[str] = None) -> str:
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
        "reportDate": String,
        "timePeriods": [
            {{
                "period": String,  // e.g., "2023", "2023-12-31", "FY2023"
                "startDate": String,
                "endDate": String,
                "metrics": {{
                    "revenue": int,
                    "costOfRevenue": int,
                    "grossProfit": int,
                    "operatingExpenses": int,
                    "operatingIncome": int,
                    "netIncome": int,
                    "earningsPerShare": float,
                    "dividendsPerShare": float,
                    "totalAssets": int,
                    "currentAssets": int,
                    "cashAndEquivalents": int,
                    "accountsReceivable": int,
                    "inventory": int,
                    "totalLiabilities": int,
                    "currentLiabilities": int,
                    "longTermDebt": int,
                    "totalEquity": int,
                    "workingCapital": int,
                    "operatingCashFlow": int,
                    "investingCashFlow": int,
                    "financingCashFlow": int,
                    "freeCashFlow": int,
                    "debtToEquityRatio": float,
                    "currentRatio": float,
                    "quickRatio": float,
                    "returnOnEquity": float,
                    "returnOnAssets": float,
                    "grossProfitMargin": float,
                    "operatingMargin": float,
                    "netProfitMargin": float,
                    "assetTurnover": float,
                    "inventoryTurnover": float,
                    "daysSalesOutstanding": float,
                    "daysInventoryOutstanding": float,
                    "daysPayableOutstanding": float,
                    "cashConversionCycle": float
                }}
            }}
        ],
        "futureCapitalExpenditures": [
            {{
                "period": String,  // The future period this capex is planned for
                "amount": int,     // Amount in dollars
                "source": {{
                    "text": String,    // The exact quotation from the text
                    "page": int,       // The page number where this was found
                    "context": String  // Brief context about what the capex is for
                }}
            }}
        ],
        "address": {{
            "street": String,
            "city": String,
            "state": String,
            "zip": String,
            "country": String
        }},
        "risks": String,
        "notes": String
    }}

    For numerical values, extract only the number (remove currency symbols and commas).
    For text blocks that appear to be risks or notes, concatenate them into a single string.
    If a value is not found on this page, use null.
    Only extract information that appears on this specific page.
    Pay special attention to the units of the values. It is common for values to be in thousands or millions.
    Also pay special attention to the alignment of the blocks. It is uncommon for values to be aligned diagonally.
    For futureCapitalExpenditures, look for statements about future capital expenditures, planned investments, or infrastructure spending.
    Include the exact quotation and context to help verify the source of the information.
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
    merged = {
        "companyName": None,
        "reportTitle": None,
        "reportDate": None,
        "timePeriods": [],
        "futureCapitalExpenditures": [],
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
    
    # Handle futureCapitalExpenditures
    all_capex = {}
    for page in pages_data:
        if not page.get("futureCapitalExpenditures"):
            continue
            
        for capex in page["futureCapitalExpenditures"]:
            # Create a unique key for this capex entry
            key = f"{capex['period']}_{capex['amount']}_{capex['source']['text']}"
            if key not in all_capex:
                all_capex[key] = capex
    
    # Convert capex dict to list and sort by period
    merged["futureCapitalExpenditures"] = sorted(
        list(all_capex.values()),
        key=lambda x: x["period"]
    )
    
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
        int_fields = [
            'revenue', 'costOfRevenue', 'grossProfit', 'operatingExpenses',
            'operatingIncome', 'netIncome', 'totalAssets', 'currentAssets',
            'cashAndEquivalents', 'accountsReceivable', 'inventory',
            'totalLiabilities', 'currentLiabilities', 'longTermDebt',
            'totalEquity', 'workingCapital', 'operatingCashFlow',
            'investingCashFlow', 'financingCashFlow', 'freeCashFlow'
        ]
        
        float_fields = [
            'earningsPerShare', 'dividendsPerShare', 'debtToEquityRatio',
            'currentRatio', 'quickRatio', 'returnOnEquity', 'returnOnAssets',
            'grossProfitMargin', 'operatingMargin', 'netProfitMargin',
            'assetTurnover', 'inventoryTurnover', 'daysSalesOutstanding',
            'daysInventoryOutstanding', 'daysPayableOutstanding', 'cashConversionCycle'
        ]
        
        # Process time periods
        if 'timePeriods' in data:
            for period in data['timePeriods']:
                if 'metrics' in period:
                    metrics = period['metrics']
                    # Convert integer fields
                    for field in int_fields:
                        if isinstance(metrics.get(field), str):
                            value = metrics[field]
                            if value:
                                value = re.sub(r'[^\d.-]', '', value)
                                try:
                                    metrics[field] = int(float(value))
                                except (ValueError, TypeError):
                                    metrics[field] = None
                    
                    # Convert float fields
                    for field in float_fields:
                        if isinstance(metrics.get(field), str):
                            value = metrics[field]
                            if value:
                                value = re.sub(r'[^\d.-]', '', value)
                                try:
                                    metrics[field] = float(value)
                                except (ValueError, TypeError):
                                    metrics[field] = None
                
                # Process futureCapitalExpenditures
                if 'futureCapitalExpenditures' in period:
                    for capex in period['futureCapitalExpenditures']:
                        # Convert amount to integer
                        if isinstance(capex.get('amount'), str):
                            value = capex['amount']
                            if value:
                                value = re.sub(r'[^\d.-]', '', value)
                                try:
                                    capex['amount'] = int(float(value))
                                except (ValueError, TypeError):
                                    capex['amount'] = None
                        
                        # Ensure source fields are present
                        if 'source' not in capex:
                            capex['source'] = {}
                        if 'text' not in capex['source']:
                            capex['source']['text'] = None
                        if 'page' not in capex['source']:
                            capex['source']['page'] = None
                        if 'context' not in capex['source']:
                            capex['source']['context'] = None
        
        return data
    
    except Exception as e:
        logger.error(f"Error cleaning LLM response: {str(e)}")
        return {}

def process_relationships_file(input_file: Path, output_file: Path, model: str, use_deepseek: bool = False, api_key: Optional[str] = None):
    """Process a single relationships file through the LLM."""
    try:
        # Skip if output file already exists
        print(input_file.name)
        if output_file.exists():
            logger.info(f"Skipping {input_file.name} - output already exists")
            return
            
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
            
            # Run through LLM
            logger.info(f"Sending page {page_num} to {model}...")
            response = run_ollama_query(prompt, model, use_deepseek, api_key)
            
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

def process_dataset(dataset_name: str, model: str = "deepseek-r1:14b", use_deepseek: bool = False, api_key: Optional[str] = None):
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
        process_relationships_file(input_file, output_file, model, use_deepseek, api_key)

def main():
    parser = argparse.ArgumentParser(description='Extract structured data from relationships using LLM')
    parser.add_argument('dataset', help='Name of the dataset directory under .data/')
    parser.add_argument('--model', default='deepseek-r1:14b',
                      help='Model to use (default: deepseek-r1:14b)')
    parser.add_argument('--use-deepseek', action='store_true',
                      help='Use DeepSeek API instead of local Ollama')
    parser.add_argument('--api-key', help='DeepSeek API key (can also be set via DEEPSEEK_API_KEY env var)')
    args = parser.parse_args()
    
    process_dataset(args.dataset, args.model, args.use_deepseek, args.api_key)

if __name__ == '__main__':
    main() 