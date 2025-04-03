#!/usr/bin/env python3
"""
Script to run mypy type checking on the codebase.
"""

import os
import sys
import subprocess
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

def run_mypy() -> None:
    """Run mypy type checking on the project"""
    print("Running mypy type checking...")
    
    # Modules to check
    modules = [
        "app.py",
        "config.py",
        "constants.py",
        "ai",
        "db",
        "routes",
        "storage",
    ]
    
    # Build command
    cmd = ["mypy", "--config-file", "mypy.ini"]
    cmd.extend(modules)
    
    # Run mypy
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    # Print results
    if result.returncode == 0:
        print("✅ No type errors found!")
    else:
        print("❌ Type errors found:")
        print(result.stdout)
        print(result.stderr)
    
    # Print summary
    error_count = result.stdout.count("error:")
    print(f"\nSummary: {error_count} errors found")
    
    # Exit with mypy's return code
    sys.exit(result.returncode)

if __name__ == "__main__":
    run_mypy() 