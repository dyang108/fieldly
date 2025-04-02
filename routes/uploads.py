import logging
from flask import Blueprint, request, jsonify, current_app
from werkzeug.utils import secure_filename

from storage import create_storage

logger = logging.getLogger(__name__)

uploads_bp = Blueprint('uploads', __name__, url_prefix='/upload')

ALLOWED_EXTENSIONS = {'csv', 'json', 'txt', 'xlsx', 'xls', 'parquet', 'pdf'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@uploads_bp.route('', methods=['POST'])
def upload_file():
    """Upload a file to a dataset"""
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
    
    if not allowed_file(file.filename):
        logger.error(f"Invalid file type: {file.filename}")
        return jsonify({'error': f'Invalid file type. Allowed types: {", ".join(ALLOWED_EXTENSIONS)}'}), 400
    
    try:
        # Get storage configuration from app config
        storage_type = current_app.config.get('STORAGE_TYPE', 'local')
        storage_config = {}
        
        if storage_type == 's3':
            storage_config = {
                'bucket_name': current_app.config.get('S3_BUCKET_NAME'),
                'aws_access_key_id': current_app.config.get('AWS_ACCESS_KEY_ID'),
                'aws_secret_access_key': current_app.config.get('AWS_SECRET_ACCESS_KEY'),
                'region_name': current_app.config.get('AWS_REGION')
            }
        else:
            storage_config = {
                'storage_path': current_app.config.get('LOCAL_STORAGE_PATH', '.data')
            }
        
        # Create storage instance
        storage = create_storage(storage_type, storage_config)
        
        # Secure the filename to prevent path traversal attacks
        filename = secure_filename(file.filename)
        
        # Save file to storage
        result = storage.save_file(dataset_name, file, filename)
        logger.info(f"File saved successfully: {result}")
        
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error saving file: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500 