import argparse
import logging
from pathlib import Path
from typing import Optional
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import pymupdf4llm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_pdf(input_file: Path, output_file: Path) -> bool:
    """Process a single PDF file and convert it to markdown."""
    try:
        # Skip if output file already exists
        if output_file.exists():
            logger.info(f"Skipping {input_file.name} - output already exists")
            return True
            
        logger.info(f"Processing {input_file.name}...")
        
        # Convert PDF to markdown using PyMuPDF4LLM
        markdown_content = pymupdf4llm.to_markdown(str(input_file))
        
        # Save as markdown
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(markdown_content)
        
        logger.info(f"Successfully processed {input_file.name} -> {output_file.name}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing {input_file}: {str(e)}")
        return False

def process_file_wrapper(args: tuple[Path, Path]) -> bool:
    """Wrapper function for parallel processing."""
    input_file, output_file = args
    return process_pdf(input_file, output_file)

def process_dataset(dataset_name: str, max_workers: Optional[int] = None) -> None:
    """Process all PDF files in a dataset directory using parallel processing."""
    data_dir = Path('../.data')
    input_dir = data_dir / dataset_name
    output_dir = data_dir / f"{dataset_name}-md"
    
    if not input_dir.exists():
        logger.error(f"Input directory {input_dir} does not exist")
        return
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all PDF files
    pdf_files = list(input_dir.glob('*.pdf'))
    
    if not pdf_files:
        logger.warning(f"No PDF files found in {input_dir}")
        return
    
    logger.info(f"Found {len(pdf_files)} PDF files to process")
    
    # Prepare arguments for parallel processing
    process_args = [
        (pdf_file, output_dir / pdf_file.name.replace('.pdf', '.md'))
        for pdf_file in pdf_files
    ]
    
    # Determine number of workers
    if max_workers is None:
        max_workers = multiprocessing.cpu_count()
    
    # Process files in parallel
    success_count = 0
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_file_wrapper, args): args[0]
            for args in process_args
        }
        
        for future in as_completed(future_to_file):
            input_file = future_to_file[future]
            try:
                if future.result():
                    success_count += 1
            except Exception as e:
                logger.error(f"Error processing {input_file}: {str(e)}")
    
    logger.info(f"Processing complete. Successfully processed {success_count}/{len(pdf_files)} files.")

def main():
    parser = argparse.ArgumentParser(description='Extract markdown from PDFs using PyMuPDF4LLM')
    parser.add_argument('dataset', help='Name of the dataset directory under .data/')
    parser.add_argument('--workers', type=int, help='Number of worker processes (default: CPU count)')
    args = parser.parse_args()
    
    process_dataset(args.dataset, args.workers)

if __name__ == '__main__':
    main() 