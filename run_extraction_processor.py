#!/usr/bin/env python3
import os
import logging
import argparse
from batch.extraction_processor import run_batch_processor

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the extraction batch processor")
    parser.add_argument("--interval", type=int, default=60, 
                        help="Polling interval in seconds (default: 60)")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level (default: INFO)")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    logger.info(f"Starting extraction batch processor with {args.interval}s polling interval")
    
    # Run the batch processor
    run_batch_processor(args.interval) 