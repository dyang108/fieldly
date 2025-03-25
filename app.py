import os
from flask import Flask, request, jsonify, send_from_directory
import boto3
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__, static_folder='static')

# Configure S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
ALLOWED_EXTENSIONS = {'csv', 'json', 'txt', 'xlsx', 'xls', 'parquet'}

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
        s3_path = f"{dataset}/{filename}"
        
        # Upload file to S3
        s3_client.upload_fileobj(
            file,
            BUCKET_NAME,
            s3_path
        )
        
        return jsonify({
            'message': 'File uploaded successfully',
            'location': s3_path
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/get-schema', methods=['GET'])
def get_schema():
    dataset = request.args.get('dataset')
    
    if not dataset:
        return jsonify({'error': 'Dataset parameter is required'}), 400
    
    try:
        # List objects in the dataset prefix
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=dataset
        )
        
        if 'Contents' not in response:
            return jsonify({'error': 'Dataset not found'}), 404
        
        files = [
            {
                'key': obj['Key'],
                'size': obj['Size'],
                'last_modified': obj['LastModified'].isoformat()
            }
            for obj in response['Contents']
        ]
        
        return jsonify({
            'dataset': dataset,
            'files': files
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True) 