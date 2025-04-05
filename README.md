# SchemaGen - Data Extraction and Schema Management System

SchemaGen is a web application for managing JSON schemas and extracting structured data from PDF documents.

## Features

- **Schema Management**: Create, view, update, and delete JSON schemas
- **Dataset Management**: Organize files into logical datasets
- **File Upload**: Upload PDF files for processing
- **AI Schema Generation**: Generate schemas using AI from natural language conversations
- **Data Extraction**: Extract structured data from PDFs according to schemas

## Setup

### Prerequisites

- Python 3.8+
- Node.js 16+
- Optional: Ollama for local LLM support

### Installation

1. Clone the repository
2. Install Python dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Install frontend dependencies:
   ```
   cd frontend && npm install
   ```
4. Build the frontend:
   ```
   cd frontend && npm run build
   ```

### Configuration

Create a `.env` file in the root directory with the following settings:

```
# Storage configuration
STORAGE_TYPE=local  # 'local' or 's3'
LOCAL_STORAGE_PATH=.data

# S3 configuration (only needed if STORAGE_TYPE=s3)
S3_BUCKET_NAME=your-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-west-1

# AI configuration
USE_LOCAL_MODEL=true  # Set to 'true' to use Ollama
OLLAMA_MODEL=deepseek-r1:14b  # Only used if USE_LOCAL_MODEL=true
OLLAMA_API_URL=http://localhost:11434/api/chat  # Ollama API URL

# DeepSeek API (only used if USE_LOCAL_MODEL=false)
DEEPSEEK_API_KEY=your-api-key

# Database configuration
DATABASE_URL=sqlite:///schemas.db
```

## Running the Application

### Production Mode

Run the Flask application:

```
python app.py
```

The application will be available at http://localhost:5000

### Development Mode

1. Start the Flask backend:
   ```
   python app.py
   ```

2. In a separate terminal, start the Vite development server:
   ```
   cd frontend && npm run dev
   ```

The frontend will be available at http://localhost:5173

## Data Extraction Pipeline

The data extraction pipeline takes PDF files, converts them to markdown, and then uses AI to extract structured data according to a schema.

### Using the Web Interface

1. Upload PDF files to a dataset
2. Create or select a schema
3. Associate the schema with the dataset
4. Navigate to the dataset view
5. Click "Extract Data" to start the extraction process

### Using the Command-Line Interface

The extraction can also be performed using the command-line tool:

```
./extract_data.py <dataset_name> [--source <source>]
```

Examples:
```
# Extract data from a local dataset
./extract_data.py financial_reports

# Extract data from an S3 dataset
./extract_data.py quarterly_reports --source s3
```

### API Endpoints

- `POST /api/extract/<source>/<dataset_name>`: Extracts data from the specified dataset
  
## Pipeline Process

1. **Dataset Selection**: The process starts by selecting a dataset with PDF files
2. **Schema Association**: A schema must be associated with the dataset
3. **PDF to Markdown Conversion**: PDFs are converted to markdown format for easier processing
4. **Data Extraction**: An AI model extracts structured data according to the schema
5. **JSON Output**: The extracted data is saved as JSON files in a new directory

The output files are stored in a directory named `<dataset_name>-extracted` within the storage location.

## Storage Model

- `.data/<dataset_name>/`: Original PDF files
- `.data/<dataset_name>-md/`: Intermediate markdown files
- `.data/<dataset_name>-extracted/`: Final JSON extraction results

# Environment Variables

The following environment variables can be set to configure the application:

- `OLLAMA_MODEL`: Model name for local Ollama (default: deepseek-r1:14b)
- `USE_LOCAL_MODEL`: Set to 'true' to use local model, 'false' for API (default: true)
- `OLLAMA_API_URL`: URL for Ollama API (default: http://localhost:11434/api/chat)
- `DATABASE_URL`: Database connection URL (default: sqlite:///schemas.db)
- `DEEPSEEK_API_KEY`: API key for DeepSeek cloud API (required if using API)
- `DEEPSEEK_API_URL`: URL for DeepSeek API (default: https://api.deepseek.com/v1/chat/completions) 

## Testing

### Frontend Testing

Run the frontend tests:

```
cd frontend && npm test
```

For coverage reports:

```
cd frontend && npm run test:coverage
```

### Backend Testing

Run the backend tests:

```
pytest
```

For coverage reports:

```
pytest --cov=. --cov-report=term --cov-report=html
```

> **Note:** When running tests with coverage, specify the coverage options directly in the command line rather than in `pytest.ini`. This avoids issues with pytest interpreting coverage options as command-line arguments.

## GitHub Actions & Local Testing

### GitHub Actions Workflows

This project uses GitHub Actions for continuous integration:

- **JavaScript Workflow**: Runs linting and tests for the frontend code.
- **Python Workflow**: Runs linting and tests for the backend code.

### Local GitHub Actions Testing

You can test GitHub Actions workflows locally using [`act`](https://github.com/nektos/act):

```bash
# Install act (on Ubuntu)
curl https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash

# Run all workflows in dry-run mode
act -n

# Run a specific workflow in dry-run mode
act -W .github/workflows/javascript.yml -n
act -W .github/workflows/python.yml -n

# Run a workflow for real
act -W .github/workflows/javascript.yml
```

### Common Issues and Fixes

When using `act` you might encounter:

1. **Node.js dependency conflicts**: Use the `--legacy-peer-deps` flag:
   ```bash
   cd frontend && npm ci --legacy-peer-deps
   ```

2. **Python package compatibility issues**: Some packages may not be available for specific Python versions. Adjust your requirements.txt as needed:
   ```bash
   # In requirements.txt
   pandas==2.0.3  # For Python 3.8 compatibility
   ```

3. **Coverage options duplication**: Specify coverage options directly in the command line rather than in `pytest.ini`:
   ```bash
   pytest --cov=. --cov-report=term --cov-report=html
   ```

## Code Quality and Linting

### Backend Linting

We use several tools to maintain code quality:

```bash
# Run all linting tools (black, flake8, isort, pylint)
python scripts/lint.py

# Run in check-only mode (no changes)
python scripts/lint.py --check

# Individual tools
black .
isort --profile black .
flake8 .
pylint .
mypy .
```

### Frontend Linting

The frontend uses ESLint for TypeScript/React code quality:

```bash
cd frontend

# Run ESLint
npm run lint
```

## Continuous Integration

This project uses GitHub Actions for continuous integration. The following checks are run on each pull request:

- Python tests (pytest)
- JavaScript tests (Jest)
- Python linting (black, flake8, isort, pylint)
- JavaScript linting (ESLint)

The GitHub Actions workflow configuration can be found in the `.github/workflows` directory. 