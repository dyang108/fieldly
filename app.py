import os
from flask import Flask, request, jsonify, send_from_directory
import boto3
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
from datetime import datetime
import pathlib

load_dotenv()

app = Flask(__name__, static_folder='static')

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

@app.route('/')
def index():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    dataset = request.form.get('dataset')
    
    if not dataset:
        return jsonify({'error': 'Dataset parameter is required'}), 400
    
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if not allowed_file(file.filename):
        return jsonify({'error': 'File type not allowed'}), 400
    
    try:
        filename = secure_filename(file.filename)
        
        if STORAGE_TYPE == 's3':
            # S3 Storage
            s3_path = f"{dataset}/{filename}"
            s3_client.upload_fileobj(file, BUCKET_NAME, s3_path)
            location = s3_path
        else:
            # Local Storage
            dataset_path = os.path.join(LOCAL_STORAGE_PATH, dataset)
            os.makedirs(dataset_path, exist_ok=True)
            file_path = os.path.join(dataset_path, filename)
            file.save(file_path)
            location = os.path.join(dataset, filename)
        
        return jsonify({
            'message': 'File uploaded successfully',
            'location': location
        }), 200
        
    except Exception as e:
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

if __name__ == '__main__':
    app.run(debug=True) 