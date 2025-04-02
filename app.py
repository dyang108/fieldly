import os
from flask import Flask, request, jsonify, send_from_directory
import boto3
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
from datetime import datetime
import pathlib
from flask_cors import CORS
from sqlalchemy import create_engine, Column, Integer, String, DateTime, JSON, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import logging
import requests
import json

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

# Create SQLAlchemy engine and session
engine = create_engine('sqlite:///schemas.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Schema(Base):
    __tablename__ = 'schemas'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    schema = Column(String, nullable=False)  # Store JSON as string
    created_at = Column(DateTime, default=datetime.utcnow)

    def get_schema(self):
        """Get the schema as a Python object"""
        return json.loads(self.schema) if self.schema else {}

    def set_schema(self, schema_data):
        """Set the schema from a Python object"""
        self.schema = json.dumps(schema_data)

class DatasetSchemaMapping(Base):
    __tablename__ = 'dataset_schema_mappings'
    id = Column(Integer, primary_key=True)
    dataset_name = Column(String, nullable=False)
    source = Column(String, nullable=False)  # 'local' or 's3'
    schema_id = Column(Integer, ForeignKey('schemas.id'))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Define relationship with Schema
    schema = relationship("Schema")

# Drop and recreate the table to ensure correct schema
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

# Create data directory if it doesn't exist
DATA_DIR = pathlib.Path(LOCAL_STORAGE_PATH)
DATA_DIR.mkdir(exist_ok=True)

# DeepSeek API Integration
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
USE_LOCAL_MODEL = os.getenv("USE_LOCAL_MODEL", "true").lower() == "true"
OLLAMA_API_URL = "http://localhost:11434/api/chat"

def generate_schema_with_deepseek(conversation):
    """Generate a schema using the DeepSeek API"""
    # Convert conversation to DeepSeek format
    messages = conversation.copy()
    
    # Add a system message if not present
    if not any(msg["role"] == "system" for msg in messages):
        messages.insert(0, {
            "role": "system",
            "content": """You are a helpful assistant that generates JSON schemas based on natural language descriptions. 
            When asked to create a schema:
            1. Analyze the user's requirements carefully
            2. Generate a comprehensive JSON schema that captures all the fields mentioned
            3. Include appropriate data types, descriptions, and constraints
            4. Return your response as valid JSON
            5. Structure your response with a 'message' field containing your explanation
               and a 'schema' field containing the JSON schema object
            6. Include a 'suggested_name' field with a good name for this schema
            """
        })
    
    # If the conversation doesn't end with a specific request for a schema, add it
    if not messages[-1]["content"].lower().strip().endswith("schema"):
        messages.append({
            "role": "user",
            "content": "Based on our conversation, please generate a complete JSON schema."
        })
    
    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}"
        }
        
        payload = {
            "model": "deepseek-chat",
            "messages": messages,
            "temperature": 0.7,
            "max_tokens": 4000
        }
        
        logger.debug(f"Sending request to DeepSeek API: {json.dumps(payload)}")
        response = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload)
        response.raise_for_status()
        
        result = response.json()
        logger.debug(f"DeepSeek API response: {result}")
        
        # Extract JSON from the response content
        content = result["choices"][0]["message"]["content"]
        
        # Try to find and parse JSON in the response
        try:
            # First, try to parse the entire response as JSON
            response_data = json.loads(content)
            return response_data
        except json.JSONDecodeError:
            # If that fails, try to extract JSON from markdown code blocks
            import re
            json_match = re.search(r'```json\s*([\s\S]*?)\s*```', content)
            if json_match:
                json_str = json_match.group(1)
                response_data = json.loads(json_str)
                return response_data
            else:
                # If no code blocks, look for JSON-like structures
                json_match = re.search(r'({[\s\S]*})', content)
                if json_match:
                    json_str = json_match.group(1)
                    response_data = json.loads(json_str)
                    return response_data
                else:
                    # If all else fails, return a basic structure with the raw content
                    return {
                        "message": "Couldn't parse JSON from response",
                        "schema": {},
                        "suggested_name": "new_schema",
                        "raw_response": content
                    }
                
    except requests.exceptions.RequestException as e:
        logger.error(f"Error calling DeepSeek API: {str(e)}")
        return {
            "message": f"Error calling DeepSeek API: {str(e)}",
            "schema": {},
            "suggested_name": "new_schema"
        }
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response: {str(e)}")
        return {
            "message": f"Error parsing schema: {str(e)}",
            "schema": {},
            "suggested_name": "new_schema",
            "raw_response": content if 'content' in locals() else ""
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "message": f"Unexpected error: {str(e)}",
            "schema": {},
            "suggested_name": "new_schema"
        }

def generate_schema_locally(conversation):
    """Generate a schema using a local mock implementation"""
    # This is a fallback when no API key is available
    logger.info("Using local schema generation (mock)")
    
    # Extract the last user message to determine schema type
    last_user_message = None
    for msg in reversed(conversation):
        if msg["role"] == "user":
            last_user_message = msg["content"].lower()
            break
    
    # Generate a simple schema based on keywords in the message
    schema = {
        "type": "object",
        "properties": {
            "id": {
                "type": "integer",
                "description": "Unique identifier"
            },
            "name": {
                "type": "string",
                "description": "Name field"
            },
            "created_at": {
                "type": "string",
                "format": "date-time",
                "description": "Creation timestamp"
            }
        },
        "required": ["id", "name"]
    }
    
    # Add some fields based on common keywords
    if last_user_message:
        if "financial" in last_user_message or "finance" in last_user_message:
            schema["properties"]["amount"] = {
                "type": "number",
                "description": "Financial amount"
            }
            schema["properties"]["currency"] = {
                "type": "string",
                "description": "Currency code"
            }
            schema["required"].append("amount")
            suggested_name = "financial_data"
            
        elif "user" in last_user_message or "profile" in last_user_message:
            schema["properties"]["email"] = {
                "type": "string",
                "format": "email",
                "description": "User email address"
            }
            schema["properties"]["age"] = {
                "type": "integer",
                "description": "User age"
            }
            suggested_name = "user_profile"
            
        elif "product" in last_user_message or "item" in last_user_message:
            schema["properties"]["price"] = {
                "type": "number",
                "description": "Product price"
            }
            schema["properties"]["description"] = {
                "type": "string",
                "description": "Product description"
            }
            schema["properties"]["inventory"] = {
                "type": "integer",
                "description": "Available inventory"
            }
            schema["required"].extend(["price", "description"])
            suggested_name = "product"
            
        else:
            suggested_name = "general_schema"
    else:
        suggested_name = "new_schema"
    
    return {
        "message": "Here is a generated schema based on your description. You can edit it in the schema editor.",
        "schema": schema,
        "suggested_name": suggested_name
    }

def generate_schema_with_local_model(conversation):
    """Generate a schema using the local DeepSeek model through Ollama"""
    try:
        # Convert conversation to Ollama format
        messages = []
        for msg in conversation:
            messages.append({
                "role": msg["role"],
                "content": msg["content"]
            })
        
        # Add system message if not present
        if not any(msg["role"] == "system" for msg in messages):
            messages.insert(0, {
                "role": "system",
                "content": """You are a helpful assistant that generates JSON schemas based on natural language descriptions. 
                When asked to create a schema:
                1. Analyze the user's requirements carefully
                2. Generate a comprehensive JSON schema that captures all the fields mentioned
                3. Include appropriate data types, descriptions, and constraints
                4. Return your response as valid JSON
                5. Structure your response with a 'message' field containing your explanation
                   and a 'schema' field containing the JSON schema object
                6. Include a 'suggested_name' field with a good name for this schema
                """
            })
        
        # If the conversation doesn't end with a specific request for a schema, add it
        if not messages[-1]["content"].lower().strip().endswith("schema"):
            messages.append({
                "role": "user",
                "content": "Based on our conversation, please generate a complete JSON schema."
            })
        
        payload = {
            "model": "deepseek-r1:14b",
            "messages": messages,
            "stream": False
        }
        
        logger.debug(f"Sending request to local Ollama API: {json.dumps(payload)}")
        response = requests.post(OLLAMA_API_URL, json=payload)
        response.raise_for_status()
        
        result = response.json()
        logger.debug(f"Local model response: {result}")
        
        # Extract content from Ollama response
        content = result["message"]["content"]
        
        # Try to find and parse JSON in the response
        try:
            # First, try to parse the entire response as JSON
            response_data = json.loads(content)
            return response_data
        except json.JSONDecodeError:
            # If that fails, try to extract JSON from markdown code blocks
            import re
            json_match = re.search(r'```json\s*([\s\S]*?)\s*```', content)
            if json_match:
                json_str = json_match.group(1)
                response_data = json.loads(json_str)
                return response_data
            else:
                # If no code blocks, look for JSON-like structures
                json_match = re.search(r'({[\s\S]*})', content)
                if json_match:
                    json_str = json_match.group(1)
                    response_data = json.loads(json_str)
                    return response_data
                else:
                    # If all else fails, return a basic structure with the raw content
                    return {
                        "message": "Couldn't parse JSON from response",
                        "schema": {},
                        "suggested_name": "new_schema",
                        "raw_response": content
                    }
                
    except requests.exceptions.RequestException as e:
        logger.error(f"Error calling local model API: {str(e)}")
        return {
            "message": f"Error calling local model API: {str(e)}",
            "schema": {},
            "suggested_name": "new_schema"
        }
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON response: {str(e)}")
        return {
            "message": f"Error parsing schema: {str(e)}",
            "schema": {},
            "suggested_name": "new_schema",
            "raw_response": content if 'content' in locals() else ""
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "message": f"Unexpected error: {str(e)}",
            "schema": {},
            "suggested_name": "new_schema"
        }

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
        logger.info("Starting GET /api/schemas request")
        schemas = session.query(Schema).all()
        logger.info(f"Successfully retrieved {len(schemas)} schemas from database")
        
        result = []
        for schema in schemas:
            try:
                schema_dict = {
                    'id': schema.id,
                    'name': schema.name,
                    'schema': schema.get_schema(),  # Convert string to JSON
                    'created_at': schema.created_at.isoformat() if schema.created_at else None
                }
                logger.debug(f"Processed schema: {schema.name} (ID: {schema.id})")
                result.append(schema_dict)
            except Exception as e:
                logger.error(f"Error processing schema {schema.id}: {str(e)}")
                continue
        
        logger.info("Successfully prepared schema response")
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in GET /api/schemas: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/schemas', methods=['POST'])
def create_schema():
    session = Session()
    try:
        logger.info("Starting POST /api/schemas request")
        data = request.get_json()
        logger.debug(f"Received data: {data}")
        
        if not data or 'name' not in data or 'schema' not in data:
            logger.error("Missing required fields in request data")
            return jsonify({'error': 'Missing required fields'}), 400
            
        schema = Schema(
            name=data['name']
        )
        schema.set_schema(data['schema'])  # Convert JSON to string
        logger.info(f"Created new schema object: {schema.name}")
        
        session.add(schema)
        logger.debug("Added schema to database session")
        
        try:
            session.commit()
            logger.info(f"Successfully committed schema {schema.id} to database")
            return jsonify({
                'id': schema.id,
                'name': schema.name,
                'schema': schema.get_schema(),  # Convert string to JSON
                'created_at': schema.created_at.isoformat() if schema.created_at else None
            }), 201
        except Exception as commit_error:
            logger.error(f"Database commit error: {str(commit_error)}", exc_info=True)
            session.rollback()
            return jsonify({'error': 'Database error'}), 500
            
    except Exception as e:
        logger.error(f"Error in POST /api/schemas: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/schemas/<int:id>', methods=['PUT'])
def update_schema(id):
    session = Session()
    try:
        data = request.get_json()
        if not data or 'schema' not in data:
            return jsonify({'error': 'Schema is required'}), 400
        
        schema = session.query(Schema).get(id)
        if not schema:
            return jsonify({'error': 'Schema not found'}), 404
        
        schema.set_schema(data['schema'])  # Convert JSON to string
        if 'name' in data:
            schema.name = data['name']
        
        session.commit()
        
        return jsonify({
            'id': schema.id,
            'name': schema.name,
            'schema': schema.get_schema(),  # Convert string to JSON
            'created_at': schema.created_at.isoformat()
        })
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/schemas/<int:id>', methods=['DELETE'])
def delete_schema(id):
    session = Session()
    try:
        schema = session.query(Schema).get(id)
        if not schema:
            return jsonify({'error': 'Schema not found'}), 404
        
        session.delete(schema)
        session.commit()
        
        return jsonify({'message': 'Schema deleted successfully'})
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/generate-schema', methods=['POST'])
def generate_schema():
    """Generate a JSON schema from a natural language conversation"""
    conversation = request.json.get('conversation', [])
    
    logger.debug(f"Received schema generation request with conversation: {conversation}")
    
    if USE_LOCAL_MODEL:
        logger.info("Using local DeepSeek model through Ollama")
        result = generate_schema_with_local_model(conversation)
    elif DEEPSEEK_API_KEY:
        logger.info("Using DeepSeek API")
        result = generate_schema_with_deepseek(conversation)
    else:
        logger.info("Using local mock implementation")
        result = generate_schema_locally(conversation)
    
    return jsonify(result)

@app.route('/api/datasets', methods=['GET'])
def get_datasets():
    try:
        logger.info("Starting GET /api/datasets request")
        local_datasets = []
        
        # List directories in .data folder
        data_dir = os.path.join(os.getcwd(), '.data')
        if os.path.exists(data_dir):
            local_datasets = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
            logger.info(f"Found {len(local_datasets)} local datasets")
        else:
            logger.info("No .data directory found")
            
        # TODO: Implement S3 dataset listing if needed
        
        result = {
            "local": local_datasets,
            "s3": []  # Placeholder for S3 datasets
        }
        
        logger.info("Successfully prepared datasets response")
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in GET /api/datasets: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/api/dataset-mappings', methods=['GET'])
def get_dataset_mappings():
    session = Session()
    try:
        logger.info("Starting GET /api/dataset-mappings request")
        mappings = session.query(DatasetSchemaMapping).all()
        logger.info(f"Successfully retrieved {len(mappings)} dataset mappings from database")
        
        result = []
        for mapping in mappings:
            schema_name = None
            if mapping.schema_id:
                schema = session.query(Schema).get(mapping.schema_id)
                if schema:
                    schema_name = schema.name
                    
            mapping_dict = {
                'id': mapping.id,
                'dataset_name': mapping.dataset_name,
                'source': mapping.source,
                'schema_id': mapping.schema_id,
                'schema_name': schema_name,
                'created_at': mapping.created_at.isoformat() if mapping.created_at else None
            }
            result.append(mapping_dict)
            
        logger.info("Successfully prepared dataset mappings response")
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in GET /api/dataset-mappings: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/dataset-mappings', methods=['POST'])
def create_or_update_mapping():
    session = Session()
    try:
        logger.info("Starting POST /api/dataset-mappings request")
        data = request.get_json()
        logger.debug(f"Received data: {data}")
        
        if not data or 'dataset_name' not in data or 'source' not in data:
            logger.error("Missing required fields in request data")
            return jsonify({'error': 'Missing required fields'}), 400
            
        # Check if mapping already exists
        existing_mapping = session.query(DatasetSchemaMapping).filter_by(
            dataset_name=data['dataset_name'],
            source=data['source']
        ).first()
        
        if existing_mapping:
            # Update existing mapping
            existing_mapping.schema_id = data.get('schema_id')
            logger.info(f"Updated mapping for dataset {data['dataset_name']}")
        else:
            # Create new mapping
            mapping = DatasetSchemaMapping(
                dataset_name=data['dataset_name'],
                source=data['source'],
                schema_id=data.get('schema_id')
            )
            session.add(mapping)
            logger.info(f"Created new mapping for dataset {data['dataset_name']}")
            
        session.commit()
        logger.info("Successfully saved dataset mapping")
        
        return jsonify({
            'success': True,
            'message': 'Dataset mapping saved successfully'
        }), 201
    except Exception as e:
        session.rollback()
        logger.error(f"Error in POST /api/dataset-mappings: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/api/apply-schema/<source>/<path:dataset_name>', methods=['POST'])
def apply_schema_to_dataset(source, dataset_name):
    session = Session()
    try:
        logger.info(f"Starting POST /api/apply-schema/{source}/{dataset_name} request")
        data = request.get_json()
        
        if not data or 'schema_id' not in data:
            logger.error("Missing schema_id in request data")
            return jsonify({'error': 'schema_id is required'}), 400
            
        schema_id = data['schema_id']
        schema = session.query(Schema).get(schema_id)
        
        if not schema:
            logger.error(f"Schema with ID {schema_id} not found")
            return jsonify({'error': 'Schema not found'}), 404
            
        # TODO: Apply the schema to the dataset files
        # This would involve reading files from the dataset directory
        # and processing them according to the schema
        
        # For now, we'll just update or create the mapping
        existing_mapping = session.query(DatasetSchemaMapping).filter_by(
            dataset_name=dataset_name,
            source=source
        ).first()
        
        if existing_mapping:
            existing_mapping.schema_id = schema_id
        else:
            mapping = DatasetSchemaMapping(
                dataset_name=dataset_name,
                source=source,
                schema_id=schema_id
            )
            session.add(mapping)
            
        session.commit()
        
        return jsonify({
            'success': True,
            'message': f'Schema "{schema.name}" has been applied to dataset "{dataset_name}"'
        })
    except Exception as e:
        session.rollback()
        logger.error(f"Error applying schema to dataset: {str(e)}", exc_info=True)
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

if __name__ == '__main__':
    app.run(debug=True) 