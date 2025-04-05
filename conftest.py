"""Root conftest.py to handle module imports properly."""
import sys
import os
from pathlib import Path

# Add the project root directory to the Python path
ROOT_DIR = Path(__file__).absolute().parent
sys.path.insert(0, str(ROOT_DIR)) 