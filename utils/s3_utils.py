import logging
import boto3
from botocore.exceptions import ClientError
from typing import List, Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

def get_s3_client(aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 region_name: Optional[str] = None) -> boto3.client:
    """
    Get an S3 client with the specified credentials
    
    Args:
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        region_name: AWS region name
        
    Returns:
        boto3.client: S3 client
    """
    try:
        # Create a session with credentials if provided
        if aws_access_key_id and aws_secret_access_key:
            session = boto3.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
            s3_client = session.client('s3')
            logger.info(f"Created S3 client with provided credentials in region {region_name}")
        else:
            # Use default credentials from environment or instance profile
            s3_client = boto3.client('s3', region_name=region_name)
            logger.info(f"Created S3 client with default credentials in region {region_name}")
        
        return s3_client
    except Exception as e:
        logger.error(f"Error creating S3 client: {str(e)}")
        raise

def list_s3_buckets(aws_access_key_id: Optional[str] = None,
                   aws_secret_access_key: Optional[str] = None,
                   region_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List all S3 buckets for the specified credentials
    
    Args:
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        region_name: AWS region name
        
    Returns:
        List[Dict[str, Any]]: List of buckets with name and creation date
    """
    try:
        s3_client = get_s3_client(aws_access_key_id, aws_secret_access_key, region_name)
        response = s3_client.list_buckets()
        
        buckets = []
        for bucket in response['Buckets']:
            buckets.append({
                'name': bucket['Name'],
                'creation_date': bucket['CreationDate'].isoformat()
            })
        
        logger.info(f"Listed {len(buckets)} S3 buckets")
        return buckets
    except Exception as e:
        logger.error(f"Error listing S3 buckets: {str(e)}")
        raise

def list_s3_objects(bucket_name: str,
                   prefix: str = '',
                   aws_access_key_id: Optional[str] = None,
                   aws_secret_access_key: Optional[str] = None,
                   region_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List objects in an S3 bucket with an optional prefix
    
    Args:
        bucket_name: Name of the S3 bucket
        prefix: Prefix to filter objects (e.g., 'folder/')
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        region_name: AWS region name
        
    Returns:
        List[Dict[str, Any]]: List of objects with key, size, and last modified date
    """
    try:
        s3_client = get_s3_client(aws_access_key_id, aws_secret_access_key, region_name)
        
        # If prefix ends with '/', treat it as a folder
        if prefix and not prefix.endswith('/'):
            folder_objects = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix + '/',
                Delimiter='/'
            )
            
            # If there are objects with this prefix as a folder, use the folder prefix
            if 'Contents' in folder_objects:
                prefix += '/'
        
        # List objects with pagination
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket_name,
            Prefix=prefix,
            Delimiter='/'
        )
        
        objects = []
        folders = set()
        
        for page in page_iterator:
            # Add files
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Skip the prefix directory itself
                    if obj['Key'] == prefix:
                        continue
                    
                    objects.append({
                        'name': obj['Key'].split('/')[-1],
                        'path': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat(),
                        'type': 'file'
                    })
            
            # Add folders
            if 'CommonPrefixes' in page:
                for common_prefix in page['CommonPrefixes']:
                    folder_name = common_prefix['Prefix'].rstrip('/').split('/')[-1]
                    folder_path = common_prefix['Prefix']
                    
                    if folder_path not in folders:
                        folders.add(folder_path)
                        objects.append({
                            'name': folder_name,
                            'path': folder_path,
                            'size': 0,
                            'last_modified': None,
                            'type': 'folder'
                        })
        
        logger.info(f"Listed {len(objects)} objects in S3 bucket '{bucket_name}' with prefix '{prefix}'")
        return objects
    except Exception as e:
        logger.error(f"Error listing objects in S3 bucket '{bucket_name}': {str(e)}")
        raise

def download_s3_file(bucket_name: str,
                    s3_key: str,
                    local_path: str,
                    aws_access_key_id: Optional[str] = None,
                    aws_secret_access_key: Optional[str] = None,
                    region_name: Optional[str] = None) -> bool:
    """
    Download a file from S3 to a local path
    
    Args:
        bucket_name: Name of the S3 bucket
        s3_key: Key of the S3 object
        local_path: Local path to save the file
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        region_name: AWS region name
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        s3_client = get_s3_client(aws_access_key_id, aws_secret_access_key, region_name)
        s3_client.download_file(bucket_name, s3_key, local_path)
        logger.info(f"Downloaded S3 object '{s3_key}' from bucket '{bucket_name}' to '{local_path}'")
        return True
    except Exception as e:
        logger.error(f"Error downloading S3 object '{s3_key}' from bucket '{bucket_name}': {str(e)}")
        return False

def upload_s3_file(local_path: str,
                  bucket_name: str,
                  s3_key: str,
                  aws_access_key_id: Optional[str] = None,
                  aws_secret_access_key: Optional[str] = None,
                  region_name: Optional[str] = None) -> bool:
    """
    Upload a file to S3
    
    Args:
        local_path: Local path of the file to upload
        bucket_name: Name of the S3 bucket
        s3_key: Key to use for the S3 object
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key
        region_name: AWS region name
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        s3_client = get_s3_client(aws_access_key_id, aws_secret_access_key, region_name)
        s3_client.upload_file(local_path, bucket_name, s3_key)
        logger.info(f"Uploaded '{local_path}' to S3 bucket '{bucket_name}' as '{s3_key}'")
        return True
    except Exception as e:
        logger.error(f"Error uploading '{local_path}' to S3 bucket '{bucket_name}': {str(e)}")
        return False 