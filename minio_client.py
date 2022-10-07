import os
import json
import logging
import argparse
import urllib3
import minio.helpers
from minio import Minio
from urllib3.exceptions import MaxRetryError
from urllib.parse import urlparse, ParseResult
from minio.error import InvalidResponseError, S3Error, ServerError
from json.decoder import JSONDecodeError


parser = argparse.ArgumentParser(description="Minio client")
parser.add_argument("credentials_path", type=str ,help="path to folder containing credentials")

class MinioClient():
    '''Minio client

    Class for convenient interaction with the Minio object storage

    Args:
     - credentials_path (str): Path to the folder containing the credentials files (default is '../credentials')
     - logger (logging.logger object): A logger can be passed, if the Minio client is used inside a class that has its own logger already (default is None which means a new logger will be created)
    '''
    def __init__(self, credentials_path='../credentials', logger=None):
        '''Constructor for Minio clients
        '''
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__ + '.' + type(self).__name__)
        self.logger.setLevel(logging.DEBUG)
        
        endpoint = None
        access_key = None
        secret_key = None

        credentials = self._read_credentials(credentials_path)
        if credentials:
            if 'url' in credentials:
                parsed_result = urlparse(credentials['url'])
                scheme = "%s://" % parsed_result.scheme
                endpoint = parsed_result.geturl().replace(scheme, '', 1)
            else:
                self.logger.error('Credentials file "{}" does not contain "url" field. No endpoint specified!'.format(os.path.join(credentials_path,'minio_credentials.json')))
                raise RuntimeError('Credentials file "{}" does not contain "url" field. No endpoint specified!'.format(os.path.join(credentials_path,'minio_credentials.json')))
            if 'accessKey' in credentials:
                access_key = credentials['accessKey']
            if 'secretKey' in credentials:
                secret_key = credentials['secretKey']

            try:
                self.client = Minio(
                    endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    http_client=self._http_client(1),
                    secure=False
                    )
                self.client.list_buckets()
            except MaxRetryError as e:
                self.logger.error('Could not connect to Minio!')
                raise RuntimeError('Could not connect to Minio!') from e
            except ValueError as e:
                self.logger.error('Invalid value in credentials file!')
                raise ValueError('Invalid value in credentials file!') from e
            else:
                self.client = Minio(
                    endpoint,
                    access_key=access_key,
                    secret_key=secret_key,
                    http_client=self._http_client(),
                    secure=False
                    )
                self.logger.info('Connected to Minio!')

    def create_bucket(self, bucket):
        '''Create a new bucket

        Create a new bucket in the Minio storage. If the specified bucket already exists no new bucket will be created.

        Args:
         - bucket (str): Bucket name
        '''
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)
            self.logger.info('Created bucket "{}"'.format(bucket))
        else:
            self.logger.info('Bucket "{}" already exists!'.format(bucket))

    def upload_file(self, bucket, minio_path, local_path):
        '''Upload a single local file to the Minio object storage

        Args:
         - bucket (str): Name of the bucket to upload the file to
         - minio_path (str): Path for the uploaded object inside the Minio storage (renaming the file is also allowed)
         - local_path (str): Path for the local file to be uploaded
        '''
        try:
            self.client.fput_object(bucket, minio_path, local_path)
        except S3Error as e:
            self.logger.error('Could not upload file "{}" to bucket "{}"'.format(local_path, bucket))
            raise RuntimeError('Could not upload file "{}" to bucket "{}"'.format(local_path, bucket)) from e
        else:
            self.logger.info('Uploaded local file "{}" to bucket "{}" to location "{}"'.format(local_path, bucket, minio_path))

    def upload_directory(self, bucket, minio_path, local_path):
        '''Upload an entire local directory to the Minio object storage recursively

        Args:
         - bucket (str): Name of the bucket to upload the directory to
         - minio_path (str): Path for the uploaded object inside the Minio storage (renaming the directory is also allowed)
         - local_path (str): Path for the local directory to be uploaded
        '''
        if os.path.isdir(local_path):
            items = os.listdir(local_path)
            items.sort()
            for item in items:
                item_local_path = os.path.join(local_path, item)
                item_minio_path = os.path.join(minio_path, item)
                if os.path.isfile(item_local_path):
                    self.upload_file(bucket, item_minio_path, item_local_path)
                elif os.path.isdir(item_local_path):
                    self.upload_directory(bucket, item_minio_path, item_local_path)
        elif os.path.isfile(local_path):
            self.logger.warn('Path "{}" is a file not a folder! Use upload_file() instead of upload_directory()'.format(local_path))
        else:
            self.logger.warn('Path "{}" not found! No upload is done!'.format(local_path))

    def download_file(self, bucket, minio_path, local_path):
        '''Download a single file from the Minio object storage

        Args:
         - bucket (str): Name of the bucket to download the file from
         - minio_path (str): Path of the object inside the Minio storage
         - local_path (str): Path for the downloaded local file (renaming the file is also allowed)
        '''
        try:
            self.client.fget_object(bucket, minio_path, local_path)
        except S3Error as e:
            self.logger.error('Could not download file "{}" from bucket "{}"'.format(minio_path, bucket))
            raise RuntimeError('Could not download file "{}" from bucket "{}"'.format(minio_path, bucket)) from e
        except ValueError as e:
            self.logger.error('Invalid input for downloading file from Minio (inputs: bucket={},minio_path={},local_path={})'.format(bucket,minio_path, local_path))
            raise RuntimeError('Invalid input for downloading file from Minio (inputs: bucket={},minio_path={},local_path={})'.format(bucket,minio_path, local_path)) from e
        else:
            self.logger.info('Downloaded file "{}" from bucket "{}" to local location "{}"'.format(minio_path, bucket, local_path))

    def download_directory(self, bucket, minio_path, local_path):
        '''Download an entire directory from the Minio object storage

        Args:
         - bucket (str): Name of the bucket to download the directory from
         - minio_path (str): Path of the object inside the Minio storage
         - local_path (str): Path for the downloaded local directory (renaming the directory is also allowed)
        '''
        for obj in self.client.list_objects(bucket, prefix=minio_path, recursive=True):
            self.download_file(bucket, obj.object_name, obj.object_name.replace(minio_path,local_path))


    def _read_credentials(self, credentials_path):
        '''Read Minio credentials

        For INTERNAL USE ONLY!
        '''
        try:
            with open(os.path.join(credentials_path,'minio_credentials.json'),'r') as f:
                credentials = json.loads(f.read())
        except FileNotFoundError as e:
            self.logger.error('Could not find credentials file "minio_credentials.json" at location: "{}"'.format(credentials_path))
            raise RuntimeError('Could not find credentials file "minio_credentials.json" at location: "{}"'.format(credentials_path)) from e
        except JSONDecodeError as e:
            self.logger.error('Could not decode credentials file "{}" (Invalid JSON)'.format(os.path.join(credentials_path,'minio_credentials.json')))
            raise RuntimeError('Could not decode credentials file "{}" (Invalid JSON)'.format(os.path.join(credentials_path,'minio_credentials.json'))) from e
        else:
            return credentials

    def _http_client(self, timeout=urllib3.Timeout.DEFAULT_TIMEOUT, retries=5):
        '''Create HTTP client for Minio

        For INTERNAL USE ONLY!
        '''
        http_client = urllib3.PoolManager(
            timeout=timeout,
            retries=urllib3.Retry(
                total=retries,
                backoff_factor=0.2,
                status_forcelist=[500,502,503,504]
            )
        )
        return http_client
    

def main():
    logging.basicConfig()
    args = parser.parse_args()
    minio_client = MinioClient(args.credentials_path)
    bucket_name = 'minio.python.api.test'
    minio_client.create_bucket(bucket_name)
    minio_client.upload_directory(bucket_name, 'test_2', '../test_2')
    minio_client.download_directory('minio.python.api.test', 'test_2/test_3', 'test')

if __name__=="__main__":
    main()