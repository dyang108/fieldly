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