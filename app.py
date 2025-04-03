import os
import logging
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

from db import init_db
from routes import register_blueprints
from constants import MODEL_CONFIGS, DEFAULT_OLLAMA_HOST, DEFAULT_OLLAMA_API_PATH, DEFAULT_DATABASE_NAME

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__,
    static_folder='frontend/dist',
    static_url_path='',
    template_folder='frontend/dist'
)

# Configure CORS
CORS(app, resources={
    r"/*": {
        "origins": ["http://localhost:5173", "http://localhost:5000"],
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Load configuration from environment variables
app.config.update(
    # Storage configuration
    STORAGE_TYPE=os.getenv('STORAGE_TYPE', 'local'),  # 'local' or 's3'
    LOCAL_STORAGE_PATH=os.getenv('LOCAL_STORAGE_PATH', '.data'),
    
    # S3 configuration
    S3_BUCKET_NAME=os.getenv('S3_BUCKET_NAME', ''),
    AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID', ''),
    AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY', ''),
    AWS_REGION=os.getenv('AWS_REGION', ''),
    
    # AI configuration
    USE_LOCAL_MODEL=os.getenv('USE_LOCAL_MODEL', 'true'),
    OLLAMA_MODEL=MODEL_CONFIGS['deepseek']['local']['model'],
    OLLAMA_API_URL=f"{DEFAULT_OLLAMA_HOST}{DEFAULT_OLLAMA_API_PATH}",
    DEEPSEEK_API_KEY=os.getenv('DEEPSEEK_API_KEY', ''),
    DEEPSEEK_API_URL=os.getenv('DEEPSEEK_API_URL', 'https://api.deepseek.com/v1/chat/completions'),
    
    # Database configuration
    DATABASE_URL=f"sqlite:///{DEFAULT_DATABASE_NAME}"
)

# Initialize the database
init_db(app.config['DATABASE_URL'], drop_first=False)

# Register blueprints
register_blueprints(app)

# Serve the React app
@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

# Handle client-side routing
@app.route('/<path:path>')
def serve(path):
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')

if __name__ == '__main__':
    app.run(debug=True) 