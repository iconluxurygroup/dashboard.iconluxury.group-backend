import os
import boto3
import urllib.parse
import logging
import mimetypes

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# S3 and R2 configuration from your code
S3_CONFIG = {
    "endpoint": "https://s3.us-east-2.amazonaws.com",
    "region": "us-east-2",
    "access_key": "AKIA2CUNLEV6V627SWI7",
    "secret_key": "QGwMNj0O0ChVEpxiEEyKu3Ye63R+58ql3iSFvHfs",
    "bucket_name": "iconluxurygroup",
    "r2_endpoint": "https://aa2f6aae69e7fb4bd8e2cd4311c411cb.r2.cloudflarestorage.com",
    "r2_access_key": "8b5a4a988c474205e0172eab5479d6f2",
    "r2_secret_key": "8ff719bbf2946c1b6a81fcf2121e1a41604a0b6f2890f308871b381e98a8d725",
    "r2_account_id": "aa2f6aae69e7fb4bd8e2cd4311c411cb",
    "r2_bucket_name": "iconluxurygroup",
}

# Function to create S3/R2 client
def get_s3_client(service='s3', logger=None, file_id=None):
    logger = logger or logging.getLogger(__name__)
    try:
        logger.info(f"Creating {service.upper()} client")
        if service == 'r2':
            client = boto3.client(
                "s3",
                region_name='auto',
                endpoint_url=S3_CONFIG['r2_endpoint'],
                aws_access_key_id=S3_CONFIG['r2_access_key'],
                aws_secret_access_key=S3_CONFIG['r2_secret_key']
            )
        else:
            client = boto3.client(
                "s3",
                region_name=S3_CONFIG['region'],
                endpoint_url=S3_CONFIG['endpoint'],
                aws_access_key_id=S3_CONFIG['access_key'],
                aws_secret_access_key=S3_CONFIG['secret_key']
            )
        logger.info(f"{service.upper()} client created successfully")
        return client
    except Exception as e:
        logger.error(f"Error creating {service.upper()} client: {e}")
        raise

# Function to double-encode plus signs in filenames
def double_encode_plus(filename, logger=None):
    logger = logger or logging.getLogger(__name__)
    logger.debug(f"Encoding filename: {filename}")
    first_pass = filename.replace('+', '%2B')
    second_pass = urllib.parse.quote(first_pass)
    logger.debug(f"Double-encoded filename: {second_pass}")
    return second_pass

# Function to upload to S3 and R2
def upload_to_s3(local_file_path, bucket_name, s3_key, r2_bucket_name=None, logger=None, file_id=None):
    logger = logger or logging.getLogger(__name__)
    result_urls = {}
    
    # Determine Content-Type
    content_type, _ = mimetypes.guess_type(local_file_path)
    if not content_type:
        content_type = 'application/octet-stream'
        logger.warning(f"Could not determine Content-Type for {local_file_path}")
    
    # Upload to AWS S3
    try:
        s3_client = get_s3_client(service='s3', logger=logger, file_id=file_id)
        logger.info(f"Uploading {local_file_path} to S3: {bucket_name}/{s3_key}")
        s3_client.upload_file(
            local_file_path,
            bucket_name,
            s3_key,
            ExtraArgs={
                'ACL': 'public-read',
                'ContentType': content_type
            }
        )
        double_encoded_key = double_encode_plus(s3_key, logger=logger)
        s3_url = f"https://{bucket_name}.s3.{S3_CONFIG['region']}.amazonaws.com/{double_encoded_key}"
        logger.info(f"Uploaded {local_file_path} to S3: {s3_url} with Content-Type: {content_type}")
        result_urls['s3'] = s3_url
    except Exception as e:
        logger.error(f"Failed to upload {local_file_path} to S3: {e}")
        raise
    
    # Upload to Cloudflare R2
    if r2_bucket_name:
        try:
            r2_client = get_s3_client(service='r2', logger=logger, file_id=file_id)
            logger.info(f"Uploading {local_file_path} to R2: {r2_bucket_name}/{s3_key}")
            r2_client.upload_file(
                local_file_path,
                r2_bucket_name,
                s3_key,
                ExtraArgs={
                    'ACL': 'public-read',
                    'ContentType': content_type
                }
            )
            double_encoded_key = double_encode_plus(s3_key, logger=logger)
            r2_url = f"https://{r2_bucket_name}.{S3_CONFIG['r2_account_id']}.r2.cloudflarestorage.com/{double_encoded_key}"
            logger.info(f"Uploaded {local_file_path} to R2: {r2_url} with Content-Type: {content_type}")
            result_urls['r2'] = r2_url
        except Exception as e:
            logger.error(f"Failed to upload {local_file_path} to R2: {e}")
            raise
    
    return result_urls

# Test the upload
def test_upload():
    try:
        # Create a dummy text file
        dummy_file_path = "dummy_test.txt"
        with open(dummy_file_path, "w") as f:
            f.write("This is a dummy text file for testing S3 and R2 uploads.")
        
        # Define upload parameters
        file_id = "test123"
        s3_key = f"test_uploads/{file_id}/dummy_test.txt"
        bucket_name = S3_CONFIG["bucket_name"]
        r2_bucket_name = S3_CONFIG["r2_bucket_name"]
        
        # Perform the upload
        urls = upload_to_s3(
            local_file_path=dummy_file_path,
            bucket_name=bucket_name,
            s3_key=s3_key,
            r2_bucket_name=r2_bucket_name,
            logger=logger,
            file_id=file_id
        )
        
        # Print results
        print("Upload Results:")
        print(f"S3 URL: {urls.get('s3')}")
        print(f"R2 URL: {urls.get('r2')}")
        
        # Verify uploads by checking if files are accessible
        import requests
        for service, url in urls.items():
            response = requests.head(url)
            if response.status_code == 200:
                print(f"{service.upper()} upload verified: File is accessible at {url}")
            else:
                print(f"{service.upper()} upload verification failed: Status code {response.status_code} for {url}")
    
    except Exception as e:
        logger.error(f"Error during test upload: {e}")
        raise
    
    finally:
        # Clean up dummy file
        if os.path.exists(dummy_file_path):
            os.remove(dummy_file_path)
            logger.info(f"Cleaned up dummy file: {dummy_file_path}")

if __name__ == "__main__":
    test_upload()