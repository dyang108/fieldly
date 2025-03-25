# Document Upload Service

A modern Flask application for uploading documents with dataset organization capabilities. Supports both local storage and AWS S3. Built with Flask, Bootstrap 5, and Dropzone.js.

## Features

- 🚀 Modern, responsive UI with Bootstrap 5
- 📁 Drag-and-drop file uploads with Dropzone.js
- 📊 Dataset organization
- 🔍 Schema viewing capabilities
- 💫 Real-time upload status and notifications
- 🎯 Support for multiple file types (CSV, JSON, TXT, Excel, Parquet)
- 🔄 Flexible storage backend (Local filesystem or AWS S3)

## Setup

1. Create a virtual environment and activate it:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file from the template:
```bash
cp .env.example .env
```

4. Configure your storage settings in `.env`:

For local storage (default):
```
STORAGE_TYPE=local
LOCAL_STORAGE_PATH=.data  # Files will be stored in this directory
```

For AWS S3 storage:
```
STORAGE_TYPE=s3
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key
AWS_REGION=your_region
S3_BUCKET_NAME=your_bucket_name
```

## Running the Application

1. Start the Flask server:
```bash
python app.py
```

2. Open your browser and navigate to:
```
http://localhost:5000
```

## Usage

1. Enter a dataset name in the input field
2. Drag and drop files into the upload zone or click to browse
3. Files will be automatically uploaded to the configured storage location:
   - Local storage: `{LOCAL_STORAGE_PATH}/{dataset}/{filename}`
   - S3 storage: `s3://{BUCKET_NAME}/{dataset}/{filename}`
4. View uploaded files by entering the dataset name in the schema section

## Supported File Types

- CSV (.csv)
- JSON (.json)
- Text (.txt)
- Excel (.xlsx, .xls)
- Parquet (.parquet)

## File Size Limits

- Maximum file size: 50MB
- Parallel uploads: 5 files simultaneously

## Error Handling

The application provides visual feedback for:
- Missing dataset names
- Invalid file types
- Upload failures
- File size violations
- Storage connectivity issues

## Security Notes

- All filenames are sanitized before upload
- Credentials are managed through environment variables
- File type restrictions are enforced both client and server-side
- Local storage path is created with secure permissions

## Storage Backend Details

### Local Storage
- Files are stored in the local filesystem
- Directory structure: `{LOCAL_STORAGE_PATH}/{dataset}/{filename}`
- Automatic creation of dataset directories
- Path traversal protection

### S3 Storage
- Files are stored in AWS S3
- Object key structure: `{dataset}/{filename}`
- Requires valid AWS credentials
- Supports large file uploads 