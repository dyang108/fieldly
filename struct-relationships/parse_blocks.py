import json
import math
from pathlib import Path
import logging
from typing import Dict, List, Tuple, Any
import argparse
from itertools import groupby
from operator import itemgetter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def calculate_distance_and_angle(block1: Dict[str, Any], block2: Dict[str, Any]) -> Tuple[float, float]:
    """Calculate the distance and angle between two blocks."""
    # Round coordinates to integers
    x1, y1 = round(block1['x0']), round(block1['top'])
    x2, y2 = round(block2['x0']), round(block2['top'])
    dx, dy = x2 - x1, y2 - y1
    distance = round(math.hypot(dx, dy))
    angle = round(math.degrees(math.atan2(dy, dx)) % 360)
    return distance, angle

def are_blocks_aligned(angle: float, distance: float, 
                      horizontal_tolerance: float = 10, 
                      vertical_tolerance: float = 10,
                      max_distance: float = 200) -> bool:
    """
    Determine if blocks are aligned based on angle and distance.
    
    Args:
        angle: Angle between blocks in degrees
        distance: Distance between blocks
        horizontal_tolerance: Maximum deviation from 0° or 180° for horizontal alignment
        vertical_tolerance: Maximum deviation from 90° or 270° for vertical alignment
        max_distance: Maximum distance to consider blocks as related
    """
    if distance > max_distance:
        return False
        
    # Normalize angle to 0-360
    angle = angle % 360
    
    # Check horizontal alignment (near 0° or 180°)
    is_horizontal = (angle < horizontal_tolerance or 
                    abs(angle - 180) < horizontal_tolerance)
    
    # Check vertical alignment (near 90° or 270°)
    is_vertical = (abs(angle - 90) < vertical_tolerance or 
                  abs(angle - 270) < vertical_tolerance)
    
    return is_horizontal or is_vertical

def find_block_relationships(blocks: List[Dict[str, Any]], 
                           max_distance: float = 200,
                           horizontal_tolerance: float = 10,
                           vertical_tolerance: float = 10,
                           max_relationships_per_block: int = 5) -> Dict[int, Dict[str, List[Dict[str, Any]]]]:
    """
    Find relationships between blocks based on their spatial arrangement, organized by page.
    
    Returns a dictionary where:
    - First level key is the page number
    - Second level key is block text
    - Value is list of related blocks on the same page
    
    Args:
        blocks: List of text blocks
        max_distance: Maximum distance to consider blocks as related
        horizontal_tolerance: Maximum deviation from 0° or 180° for horizontal alignment
        vertical_tolerance: Maximum deviation from 90° or 270° for vertical alignment
        max_relationships_per_block: Maximum number of relationships to store per block
    """
    # Sort blocks by page number
    blocks.sort(key=itemgetter('page_number'))
    relationships_by_page = {}
    
    # Group blocks by page number
    for page_num, page_blocks in groupby(blocks, key=itemgetter('page_number')):
        page_blocks = list(page_blocks)
        page_relationships = {}
        
        # Sort blocks by y-coordinate (top to bottom)
        page_blocks.sort(key=lambda x: round(x['top']))
        
        for i, block1 in enumerate(page_blocks):
            text1 = block1['text'].strip()
            if not text1 or len(text1) < 2:  # Skip very short text
                continue
                
            relationships = []
            
            # Look at a larger window of blocks (increased from ±10 to ±20)
            start_idx = max(0, i - 20)
            end_idx = min(len(page_blocks), i + 20)
            
            for j in range(start_idx, end_idx):
                if i == j:
                    continue
                    
                block2 = page_blocks[j]
                text2 = block2['text'].strip()
                if not text2 or len(text2) < 2:  # Skip very short text
                    continue
                
                distance, angle = calculate_distance_and_angle(block1, block2)
                
                # Normalize angle to 0-360
                angle = angle % 360
                
                # Determine relationship type based on angle and distance
                if distance <= max_distance:
                    # Check for horizontal alignment (near 0° or 180°)
                    if angle < horizontal_tolerance or abs(angle - 180) < horizontal_tolerance:
                        relationship = {
                            'text': text2,
                            'distance': distance,
                            'angle': angle,
                            'alignment': 'horizontal',
                            'type': 'same_line',
                            'coordinates': {
                                'x0': round(block2['x0']),
                                'x1': round(block2['x1']),
                                'top': round(block2['top']),
                                'bottom': round(block2['bottom'])
                            }
                        }
                        relationships.append(relationship)
                    
                    # Check for vertical alignment (near 90° or 270°)
                    elif abs(angle - 90) < vertical_tolerance or abs(angle - 270) < vertical_tolerance:
                        relationship = {
                            'text': text2,
                            'distance': distance,
                            'angle': angle,
                            'alignment': 'vertical',
                            'type': 'same_column',
                            'coordinates': {
                                'x0': round(block2['x0']),
                                'x1': round(block2['x1']),
                                'top': round(block2['top']),
                                'bottom': round(block2['bottom'])
                            }
                        }
                        relationships.append(relationship)
                    
                    # Check for diagonal relationships (45°, 135°, 225°, 315°)
                    elif any(abs(angle - a) < 15 for a in [45, 135, 225, 315]):
                        relationship = {
                            'text': text2,
                            'distance': distance,
                            'angle': angle,
                            'alignment': 'diagonal',
                            'type': 'diagonal_relation',
                            'coordinates': {
                                'x0': round(block2['x0']),
                                'x1': round(block2['x1']),
                                'top': round(block2['top']),
                                'bottom': round(block2['bottom'])
                            }
                        }
                        relationships.append(relationship)
                    
                    # Check for nearby blocks regardless of alignment
                    elif distance < max_distance / 2:
                        relationship = {
                            'text': text2,
                            'distance': distance,
                            'angle': angle,
                            'alignment': 'proximate',
                            'type': 'nearby',
                            'coordinates': {
                                'x0': round(block2['x0']),
                                'x1': round(block2['x1']),
                                'top': round(block2['top']),
                                'bottom': round(block2['bottom'])
                            }
                        }
                        relationships.append(relationship)
            
            # Sort relationships by distance and type priority
            if relationships:
                # Define type priorities (lower number = higher priority)
                type_priority = {
                    'same_line': 1,
                    'same_column': 2,
                    'diagonal_relation': 3,
                    'nearby': 4
                }
                
                # Sort by type priority first, then by distance
                relationships.sort(key=lambda x: (type_priority.get(x['type'], 5), x['distance']))
                relationships = relationships[:max_relationships_per_block]
                page_relationships[text1] = relationships
        
        # Only add page if it has relationships
        if page_relationships:
            relationships_by_page[page_num] = page_relationships
    
    return relationships_by_page

def process_blocks_file(input_file: Path, output_file: Path,
                       max_distance: float = 200,
                       horizontal_tolerance: float = 10,
                       vertical_tolerance: float = 10,
                       max_relationships_per_block: int = 5):
    """Process a single blocks file and find relationships."""
    try:
        # Skip if output file already exists
        if output_file.exists():
            logger.info(f"Skipping {input_file.name} - output already exists")
            return
            
        logger.info(f"Processing {input_file.name}...")
        
        # Read blocks file
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not data.get('blocks'):
            logger.warning(f"No blocks found in {input_file}")
            return
        
        # Find relationships
        relationships = find_block_relationships(
            data['blocks'],
            max_distance=max_distance,
            horizontal_tolerance=horizontal_tolerance,
            vertical_tolerance=vertical_tolerance,
            max_relationships_per_block=max_relationships_per_block
        )
        
        # Save relationships
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump({
                'filename': data['filename'],
                'relationships_by_page': relationships
            }, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Processed {input_file.name} -> {output_file.name}")
        
    except Exception as e:
        logger.error(f"Error processing {input_file}: {str(e)}")

def process_dataset(dataset_name: str,
                   max_distance: float = 200,
                   horizontal_tolerance: float = 10,
                   vertical_tolerance: float = 10,
                   max_relationships_per_block: int = 5):
    """Process all block files in a dataset."""
    data_dir = Path('../.data')
    input_dir = data_dir / f"{dataset_name}-parsed"
    output_dir = data_dir / f"{dataset_name}-relationships"
    
    if not input_dir.exists():
        logger.error(f"Input directory {input_dir} does not exist")
        return
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    block_files = list(input_dir.glob('*_blocks.json'))
    
    if not block_files:
        logger.warning(f"No block files found in {input_dir}")
        return
    
    logger.info(f"Found {len(block_files)} block files to process")
    
    for input_file in block_files:
        output_file = output_dir / input_file.name.replace('_blocks.json', '_relationships.json')
        process_blocks_file(
            input_file, 
            output_file,
            max_distance=max_distance,
            horizontal_tolerance=horizontal_tolerance,
            vertical_tolerance=vertical_tolerance,
            max_relationships_per_block=max_relationships_per_block
        )

def main():
    parser = argparse.ArgumentParser(description='Analyze spatial relationships between text blocks')
    parser.add_argument('dataset', help='Name of the dataset directory under .data/')
    parser.add_argument('--max-distance', type=float, default=200,
                      help='Maximum distance to consider blocks as related')
    parser.add_argument('--horizontal-tolerance', type=float, default=10,
                      help='Angle tolerance for horizontal alignment (degrees)')
    parser.add_argument('--vertical-tolerance', type=float, default=10,
                      help='Angle tolerance for vertical alignment (degrees)')
    parser.add_argument('--max-relationships', type=int, default=5,
                      help='Maximum number of relationships to store per block')
    args = parser.parse_args()
    
    process_dataset(
        args.dataset,
        max_distance=args.max_distance,
        horizontal_tolerance=args.horizontal_tolerance,
        vertical_tolerance=args.vertical_tolerance,
        max_relationships_per_block=args.max_relationships
    )

if __name__ == '__main__':
    main() 