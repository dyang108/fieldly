import os
import json
import pdfplumber
from pathlib import Path
import logging
from typing import Dict, List, Any
import argparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert a value to float."""
    try:
        return float(value) if value is not None else default
    except (ValueError, TypeError):
        return default

def extract_blocks_from_pdf(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Extract text blocks from a PDF file using pdfplumber.
    Returns a list of blocks with their properties.
    """
    all_blocks = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                logger.debug(f"Processing page {page_num}")
                
                try:
                    # First try to get raw words to debug
                    raw_words = page.extract_words(
                        keep_blank_chars=True,
                        x_tolerance=3,
                        y_tolerance=3,
                        extra_attrs=['fontname', 'size']
                    )
                    
                    if raw_words:
                        logger.debug(f"Sample raw word data: {raw_words[0]}")
                    
                    # Now process each word with safety checks
                    for word in raw_words:
                        block = {
                            'text': str(word.get('text', '')),
                            'page_number': page_num,
                            'fontname': str(word.get('fontname', '')),
                            'size': safe_float(word.get('size')),
                            # Coordinates with safety checks
                            'x0': safe_float(word.get('x0')),
                            'x1': safe_float(word.get('x1')),
                            'y0': safe_float(word.get('top')),  # Try 'top' instead of 'y0'
                            'y1': safe_float(word.get('bottom')),  # Try 'bottom' instead of 'y1'
                            'top': safe_float(word.get('top')),
                            'bottom': safe_float(word.get('bottom')),
                            'doctop': safe_float(word.get('doctop')),
                            # Add additional useful properties
                            'upright': bool(word.get('upright', True)),
                            'direction': str(word.get('direction', 'ltr'))
                        }
                        all_blocks.append(block)
                
                except Exception as page_error:
                    logger.error(f"Error processing page {page_num} in {pdf_path}: {str(page_error)}")
                    # Try alternative extraction method
                    try:
                        # Fallback to simpler extraction
                        text = page.extract_text()
                        if text:
                            block = {
                                'text': text,
                                'page_number': page_num,
                                'extraction_type': 'fallback'
                            }
                            all_blocks.append(block)
                    except Exception as fallback_error:
                        logger.error(f"Fallback extraction failed for page {page_num}: {str(fallback_error)}")
        
        if not all_blocks:
            # If no blocks extracted, try one final fallback to get any text
            try:
                text = pdf.pages[0].extract_text()
                if text:
                    logger.warning(f"Using basic text extraction for {pdf_path}")
                    all_blocks.append({
                        'text': text,
                        'page_number': 1,
                        'extraction_type': 'basic'
                    })
            except Exception as e:
                logger.error(f"Basic extraction failed for {pdf_path}: {str(e)}")
        
        return all_blocks
    
    except Exception as e:
        logger.error(f"Error processing {pdf_path}: {str(e)}")
        return []

def process_dataset(dataset_name: str):
    """
    Process all PDFs in a dataset directory and save extracted blocks.
    """
    # Setup paths
    data_dir = Path('.data')
    input_dir = data_dir / dataset_name
    output_dir = data_dir / f"{dataset_name}-parsed"
    
    # Ensure input directory exists
    if not input_dir.exists():
        logger.error(f"Dataset directory {input_dir} does not exist")
        return
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Process each PDF file
    pdf_files = list(input_dir.glob('*.pdf'))
    
    if not pdf_files:
        logger.warning(f"No PDF files found in {input_dir}")
        return
    
    logger.info(f"Found {len(pdf_files)} PDF files to process")
    
    for pdf_path in pdf_files:
        logger.info(f"Processing {pdf_path.name}")
        
        # Extract blocks from PDF
        blocks = extract_blocks_from_pdf(str(pdf_path))
        
        if not blocks:
            logger.warning(f"No blocks extracted from {pdf_path.name}")
            continue
        
        # Create output file path
        output_file = output_dir / f"{pdf_path.stem}_blocks.json"
        
        # Save blocks to JSON file
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'filename': pdf_path.name,
                    'total_blocks': len(blocks),
                    'blocks': blocks
                }, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(blocks)} blocks to {output_file}")
        
        except Exception as e:
            logger.error(f"Error saving blocks for {pdf_path.name}: {str(e)}")

def process_pdf_file(input_file: Path, output_file: Path):
    """Process a single PDF file and extract text blocks."""
    try:
        # Skip if output file already exists
        if output_file.exists():
            logger.info(f"Skipping {input_file.name} - output already exists")
            return
            
        logger.info(f"Processing {input_file.name}...")
        
        # Extract text blocks
        blocks = extract_blocks_from_pdf(str(input_file))
        
        # Save blocks
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'filename': input_file.name,
                'blocks': blocks
            }, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Processed {input_file.name} -> {output_file.name}")
        
    except Exception as e:
        logger.error(f"Error processing {input_file}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Process PDFs and extract text blocks')
    parser.add_argument('dataset', help='Name of the dataset directory under .data/')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    logger.info(f"Starting PDF processing for dataset: {args.dataset}")
    process_dataset(args.dataset)
    logger.info("Processing complete")

if __name__ == '__main__':
    main() 