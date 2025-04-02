import os
from flask import Flask, request, jsonify, send_from_directory
import boto3
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
from datetime import datetime
import pathlib
from flask_cors import CORS
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

# Create Flask app with React build folder as static folder
app = Flask(__name__, 
    static_folder='frontend/dist',
    static_url_path='',
    template_folder='frontend/dist'
)

# Configure CORS to allow requests from both the React dev server and the production server
CORS(app, resources={
    r"/*": {
        "origins": ["http://localhost:5173", "http://localhost:5000"],
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Configuration
STORAGE_TYPE = os.getenv('STORAGE_TYPE', 'local')  # 'local' or 's3'
LOCAL_STORAGE_PATH = os.getenv('LOCAL_STORAGE_PATH', '.data')

# Configure S3 client if using S3
if STORAGE_TYPE == 's3':
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )
    BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
else:
    # Create local storage directory if it doesn't exist
    pathlib.Path(LOCAL_STORAGE_PATH).mkdir(parents=True, exist_ok=True)

ALLOWED_EXTENSIONS = {'csv', 'json', 'txt', 'xlsx', 'xls', 'parquet', 'pdf'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Database setup
engine = create_engine('sqlite:///schemas.db')
Base = declarative_base()
Session = sessionmaker(bind=engine)

class Schema(Base):
    __tablename__ = 'schemas'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    content = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

Base.metadata.create_all(engine)

# Create data directory if it doesn't exist
DATA_DIR = pathlib.Path(LOCAL_STORAGE_PATH)
DATA_DIR.mkdir(exist_ok=True)

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

@app.route('/upload', methods=['POST'])
def upload_file():
    logger.debug("Received upload request")
    logger.debug(f"Request headers: {dict(request.headers)}")
    logger.debug(f"Request form data: {dict(request.form)}")
    logger.debug(f"Request files: {dict(request.files)}")
    
    if 'file' not in request.files:
        logger.error("No file part in request")
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    dataset_name = request.form.get('dataset_name')
    
    logger.debug(f"Dataset name: {dataset_name}")
    logger.debug(f"File name: {file.filename}")
    
    if not dataset_name:
        logger.error("No dataset name provided")
        return jsonify({'error': 'No dataset name provided'}), 400
    
    if file.filename == '':
        logger.error("No selected file")
        return jsonify({'error': 'No selected file'}), 400
    
    if not file.filename.endswith('.pdf'):
        logger.error(f"Invalid file type: {file.filename}")
        return jsonify({'error': 'Only PDF files are allowed'}), 400
    
    try:
        # Create dataset directory if it doesn't exist
        dataset_dir = DATA_DIR / dataset_name
        dataset_dir.mkdir(exist_ok=True)
        logger.debug(f"Created/verified dataset directory: {dataset_dir}")
        
        # Save file
        filename = secure_filename(file.filename)
        file_path = dataset_dir / filename
        file.save(file_path)
        logger.info(f"File saved successfully: {file_path}")
        
        return jsonify({
            'message': 'File uploaded successfully',
            'filename': filename,
            'path': str(file_path)
        })
    except Exception as e:
        logger.error(f"Error saving file: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/get-schema', methods=['GET'])
def get_schema():
    dataset = request.args.get('dataset')
    
    if not dataset:
        return jsonify({'error': 'Dataset parameter is required'}), 400
    
    try:
        if STORAGE_TYPE == 's3':
            # List objects in S3
            response = s3_client.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix=dataset
            )
            
            if 'Contents' not in response:
                return jsonify({'dataset': dataset, 'files': []}), 200
            
            files = [
                {
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat()
                }
                for obj in response['Contents']
            ]
        else:
            # List files in local directory
            dataset_path = os.path.join(LOCAL_STORAGE_PATH, dataset)
            if not os.path.exists(dataset_path):
                return jsonify({'dataset': dataset, 'files': []}), 200
            
            files = []
            for file_path in pathlib.Path(dataset_path).glob('*'):
                if file_path.is_file():
                    stat = file_path.stat()
                    files.append({
                        'key': os.path.join(dataset, file_path.name),
                        'size': stat.st_size,
                        'last_modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                    })
        
        return jsonify({
            'dataset': dataset,
            'files': files
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/schemas', methods=['GET'])
def get_schemas():
    session = Session()
    try:
        schemas = session.query(Schema).all()
        return jsonify([{
            'id': schema.id,
            'name': schema.name,
            'content': schema.content,
            'created_at': schema.created_at.isoformat(),
            'updated_at': schema.updated_at.isoformat()
        } for schema in schemas])
    finally:
        session.close()

@app.route('/api/schemas', methods=['POST'])
def create_schema():
    data = request.get_json()
    if not data or 'name' not in data or 'content' not in data:
        return jsonify({'error': 'Missing required fields'}), 400
        
    session = Session()
    try:
        schema = Schema(
            name=data['name'],
            content=data['content']
        )
        session.add(schema)
        session.commit()
        return jsonify({
            'id': schema.id,
            'name': schema.name,
            'content': schema.content,
            'created_at': schema.created_at.isoformat(),
            'updated_at': schema.updated_at.isoformat()
        }), 201
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/schemas/<int:schema_id>', methods=['PUT'])
def update_schema(schema_id):
    data = request.get_json()
    if not data or 'name' not in data or 'content' not in data:
        return jsonify({'error': 'Missing required fields'}), 400
        
    session = Session()
    try:
        schema = session.query(Schema).get(schema_id)
        if not schema:
            return jsonify({'error': 'Schema not found'}), 404
            
        schema.name = data['name']
        schema.content = data['content']
        session.commit()
        return jsonify({
            'id': schema.id,
            'name': schema.name,
            'content': schema.content,
            'created_at': schema.created_at.isoformat(),
            'updated_at': schema.updated_at.isoformat()
        })
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/schemas/<int:schema_id>', methods=['DELETE'])
def delete_schema(schema_id):
    session = Session()
    try:
        schema = session.query(Schema).get(schema_id)
        if not schema:
            return jsonify({'error': 'Schema not found'}), 404
            
        session.delete(schema)
        session.commit()
        return '', 204
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

if __name__ == '__main__':
    app.run(debug=True) 