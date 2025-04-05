#!/usr/bin/env python3
"""
Script to run all linting tools: black, flake8, isort, and pylint.
Can be called with the --check flag to only check without making changes.
"""

import argparse
import subprocess
import sys
from pathlib import Path

# Get the repository root directory
REPO_ROOT = Path(__file__).parent.parent.absolute()

# Directories to lint
LINT_PATHS = [
    ".",
    "ai",
    "db",
    "routes",
    "scripts",
    "storage",
    "utils",
    "tests",
]

# Files to exclude
EXCLUDE = [
    "__pycache__",
    ".git",
    ".env",
    ".venv",
    "venv",
    "node_modules",
    ".mypy_cache",
    ".pytest_cache",
    ".coverage",
    "*.pyc",
]

def run_command(cmd, check_only=False):
    """Run a shell command and return exit code."""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.stdout:
        print(result.stdout)
    
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    
    if result.returncode != 0:
        print(f"Command failed with exit code {result.returncode}")
        
    return result.returncode

def main():
    """Run all linting tools."""
    parser = argparse.ArgumentParser(description="Run all linting tools.")
    parser.add_argument("--check", action="store_true", help="Only check, don't make changes")
    args = parser.parse_args()
    
    # Change to the repository root
    os.chdir(REPO_ROOT)
    
    # Construct the paths list
    paths = []
    for path in LINT_PATHS:
        if Path(path).exists():
            paths.append(path)
    
    # Exit code will be non-zero if any check fails
    exit_code = 0
    
    # Run isort
    isort_cmd = ["isort", "--profile", "black"]
    if args.check:
        isort_cmd.append("--check")
    isort_cmd.extend(paths)
    
    exit_code |= run_command(isort_cmd, args.check)
    
    # Run black
    black_cmd = ["black"]
    if args.check:
        black_cmd.append("--check")
    black_cmd.extend(paths)
    
    exit_code |= run_command(black_cmd, args.check)
    
    # Run flake8 (check-only tool)
    flake8_cmd = ["flake8"] + paths
    exit_code |= run_command(flake8_cmd, True)
    
    # Run pylint
    pylint_cmd = ["pylint"] + paths
    exit_code |= run_command(pylint_cmd, True)
    
    if exit_code:
        print("Linting failed")
    else:
        print("All linting checks passed!")
    
    return exit_code

if __name__ == "__main__":
    # Import os here to avoid import issues
    import os
    sys.exit(main()) 