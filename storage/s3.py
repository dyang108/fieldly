import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from typing import List, Dict, Any, BinaryIO

from .base import StorageInterface


class S3Storage(StorageInterface):
    """S3 storage implementation"""
    
    def __init__(
        self, 
        bucket_name: str,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None, 
        region_name: str = None
    ):
        """
        Initialize S3 storage
        
        Args:
            bucket_name: S3 bucket name
            aws_access_key_id: AWS access key ID (or from env vars)
            aws_secret_access_key: AWS secret access key (or from env vars)
            region_name: AWS region (or from env vars)
        """
        self.bucket_name = bucket_name
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
    
    def save_file(self, dataset_name: str, file_obj: BinaryIO, filename: str) -> Dict[str, Any]:
        """
        Upload a file to S3
        
        Args:
            dataset_name: Name of the dataset (prefix in S3)
            file_obj: File object to upload
            filename: Name of the file
            
        Returns:
            Dict with file info
        """
        s3_key = f"{dataset_name}/{filename}"
        
        try:
            # Reset file pointer to beginning
            file_obj.seek(0)
            
            # Upload to S3
            self.s3_client.upload_fileobj(file_obj, self.bucket_name, s3_key)
            
            # Get file info
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            
            return {
                'message': 'File uploaded successfully',
                'filename': filename,
                'path': s3_key,
                'size': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified', datetime.now()).isoformat()
            }
        except ClientError as e:
            raise Exception(f"Error uploading file to S3: {str(e)}")
    
    def list_datasets(self) -> List[str]:
        """
        List all datasets (top-level prefixes) in the S3 bucket
        
        Returns:
            List of dataset names (prefixes)
        """
        try:
            # List top-level prefixes (common prefixes)
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Delimiter='/'
            )
            
            datasets = []
            
            # Extract common prefixes (directories)
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    # Remove trailing slash
                    dataset_name = prefix['Prefix'].rstrip('/')
                    datasets.append(dataset_name)
            
            return datasets
            
        except ClientError as e:
            raise Exception(f"Error listing datasets from S3: {str(e)}")
    
    def list_files(self, dataset_name: str) -> List[Dict[str, Any]]:
        """
        List all files in a dataset (prefix) in the S3 bucket
        
        Args:
            dataset_name: Name of the dataset (prefix)
            
        Returns:
            List of file info dicts
        """
        try:
            # Ensure dataset_name ends with a slash to list only contents
            prefix = f"{dataset_name}/"
            if dataset_name.endswith('/'):
                prefix = dataset_name
            
            # List objects with the prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            files = []
            
            # Extract file info
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Skip the directory object itself
                    if obj['Key'] == prefix:
                        continue
                    
                    # Extract filename from key
                    filename = os.path.basename(obj['Key'])
                    
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat(),
                        'name': filename
                    })
            
            return files
            
        except ClientError as e:
            raise Exception(f"Error listing files from S3: {str(e)}")
    
    def get_file(self, dataset_name: str, filename: str) -> Any:
        """
        Get a file from S3
        
        Args:
            dataset_name: Name of the dataset (prefix)
            filename: Name of the file
            
        Returns:
            BytesIO object containing file data
        """
        from io import BytesIO
        
        s3_key = f"{dataset_name}/{filename}"
        
        try:
            # Create a BytesIO object to store the file data
            file_data = BytesIO()
            
            # Download the file into the BytesIO object
            self.s3_client.download_fileobj(self.bucket_name, s3_key, file_data)
            
            # Reset the file pointer to the beginning
            file_data.seek(0)
            
            return file_data
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            raise Exception(f"Error getting file from S3: {str(e)}")
    
    def delete_file(self, dataset_name: str, filename: str) -> bool:
        """
        Delete a file from S3
        
        Args:
            dataset_name: Name of the dataset (prefix)
            filename: Name of the file
            
        Returns:
            True if successful, False otherwise
        """
        s3_key = f"{dataset_name}/{filename}"
        
        try:
            # Delete the object
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            return True
            
        except ClientError:
            return False 