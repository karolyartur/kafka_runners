import os
import re
import json
import logging
import argparse
import urllib3
import s3fs
import minio.helpers
from minio import Minio
from urllib3.exceptions import MaxRetryError
from urllib.parse import urlparse, ParseResult
from minio.error import InvalidResponseError, S3Error, ServerError
from json.decoder import JSONDecodeError

import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from classproperty import classproperty, ClassPropertyMetaClass


parser = argparse.ArgumentParser(description="Minio client")
parser.add_argument("credentials_path", type=str ,help="path to folder containing credentials")

class MinioClient(metaclass=ClassPropertyMetaClass):
    '''Minio client

    Class for convenient interaction with the Minio object storage

    It can either be used by making an instance of it:

    minio = MinioClient()
    minio.list_buckets()
    ...

    Or as a class:

    MinioClient.connect()
    MinioClient.list_buckets()
    ...

    Args (if used by creating an instance):
     - credentials_path: Path to the folder containing the credentials files (default is '../credentials')
     - logger: A logger can be passed, if the Minio client is used inside a class that has its own logger already (default is None which means a new logger will be created)
    '''
    logging.basicConfig()
    _logger = logging.getLogger(__name__ + '.MinioClient')
    _logger.setLevel(logging.DEBUG)
    _client = None
    _debug = True
    _raise_errors = True
    _s3 = None


    def __init__(self, credentials_path: str='../credentials', logger: logging.Logger=None):
        '''Constructor for Minio clients
        '''
        # Establish connection when the instance is created
        self.connect(credentials_path, logger)


    # Logger class property
    @classproperty
    def logger(self) -> logging.Logger:
        return self._logger

    @logger.setter
    def logger(self, value: logging.Logger) -> None:
        if isinstance(value, logging.Logger):
            self._logger = value
        else:
            raise TypeError('"logger" must be of type "logging.Logger"')


    # Client class property
    @classproperty
    def client(self) -> Minio:
        return self._client

    @client.setter
    def client(self, value: Minio) -> None:
        if isinstance(value, Minio):
            self._client = value
        else:
            raise TypeError('"client" must be an instance of the "Minio" class')


    # Depug class property (switch between DEBUG and WARN logging levels)
    @classproperty
    def debug(self) -> bool:
        return self._debug
    
    @debug.setter
    def debug(self, value: bool) -> None:
        if isinstance(value, bool):
            self._debug = value
            if self.logger:
                if value:
                    self.logger.setLevel(logging.DEBUG)
                else:
                    self.logger.setLevel(logging.WARN)
        else:
            raise TypeError('"debug" must be a boolean')


    # Raise errors class property (Enable raising errors)
    @classproperty
    def raise_errors(self) -> bool:
        return self._raise_errors

    @raise_errors.setter
    def raise_errors(self, value: bool) -> None:
        if isinstance(value, bool):
            self._raise_errors = value
        else:
            raise TypeError('"raise_errors" must be a boolean')


    @classmethod
    def connect(self, credentials_path: str='../credentials', logger: logging.Logger=None) -> None:
        '''Connect to Minio

        Args:
         - credentials_path: Path to the folder containing the credentials files (default is '../credentials')
         - logger: A logger can be passed (default is None which means a new logger will be created)
        '''
        # Set logger
        if logger:
            self.logger = logger
        if self.debug:
            self.logger.setLevel(logging.DEBUG)

        endpoint = None
        access_key = None
        secret_key = None

        credentials = self._read_credentials(credentials_path)
        if credentials:
            # Parse credentials
            if 'url' in credentials:
                parsed_result = urlparse(credentials['url'])
                scheme = "%s://" % parsed_result.scheme
                endpoint = parsed_result.geturl().replace(scheme, '', 1)
            else:
                self.logger.error('Credentials file "{}" does not contain "url" field. No endpoint specified!'.format(os.path.join(credentials_path,'minio_credentials.json')))
                if self.raise_errors:
                    raise RuntimeError('Credentials file "{}" does not contain "url" field. No endpoint specified!'.format(os.path.join(credentials_path,'minio_credentials.json')))
            if 'accessKey' in credentials:
                access_key = credentials['accessKey']
            if 'secretKey' in credentials:
                secret_key = credentials['secretKey']

            # Setup S3FS
            self._s3 = s3fs.S3FileSystem(key=access_key, secret=secret_key, client_kwargs={'endpoint_url':credentials['url']}, use_listings_cache=False)

            # Setup Minio
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
                if self.raise_errors:
                    raise RuntimeError('Could not connect to Minio!') from e
            except ValueError as e:
                self.logger.error('Invalid value in credentials file!')
                if self.raise_errors:
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


    @classmethod
    def list_buckets(self) -> list[str]|None:
        '''List all buckets in the object storage
        '''
        try:
            return self.client.list_buckets()
        except MaxRetryError as e:
            self.logger.error('Could not connect to Minio!')
            if self.raise_errors:
                raise RuntimeError('Could not connect to Minio!') from e
        except ValueError as e:
            self.logger.error('Invalid value in credentials file!')
            if self.raise_errors:
                raise ValueError('Invalid value in credentials file!') from e


    @classmethod
    def create_bucket(self, bucket: str) -> None:
        '''Create a new bucket

        Create a new bucket in the Minio storage. If the specified bucket already exists no new bucket will be created.

        Args:
         - bucket: Bucket name
        '''
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)
            self.logger.info('Created bucket "{}"'.format(bucket))
        else:
            self.logger.info('Bucket "{}" already exists!'.format(bucket))


    @classmethod
    def list_objects(self, bucket: str, minio_path: str, recursive: bool=False, start_after: str=None) -> list[str]:
        '''List objects in the Minio object storage

        Args:
         - bucket: Name of the bucket in which the objects are
         - minio_path: Path for the folder (in the MinIO object storage) in which the objects are
         - recursive: List objects in subdirectories
         - start_after: List objects after this key name

        Returns:
         - objects: List of object names
        '''
        return [o.object_name for o in self.client.list_objects(bucket, minio_path, recursive, start_after)]


    @classmethod
    def upload_file(self, bucket: str, minio_path: str, local_path: str) -> None:
        '''Upload a single local file to the Minio object storage

        Args:
         - bucket: Name of the bucket to upload the file to
         - minio_path: Path for the uploaded object inside the Minio storage (renaming the file is also allowed)
         - local_path: Path for the local file to be uploaded
        '''
        try:
            self.client.fput_object(bucket, minio_path, local_path)
        except S3Error as e:
            self.logger.error('Could not upload file "{}" to bucket "{}"'.format(local_path, bucket))
            if self.raise_errors:
                raise RuntimeError('Could not upload file "{}" to bucket "{}"'.format(local_path, bucket)) from e
        else:
            self.logger.info('Uploaded local file "{}" to bucket "{}" to location "{}"'.format(local_path, bucket, minio_path))


    @classmethod
    def upload_directory(self, bucket: str, minio_path: str, local_path: str) -> None:
        '''Upload an entire local directory to the Minio object storage recursively

        Args:
         - bucket: Name of the bucket to upload the directory to
         - minio_path: Path for the uploaded object inside the Minio storage (renaming the directory is also allowed)
         - local_path: Path for the local directory to be uploaded
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


    @classmethod
    def download_file(self, bucket: str, minio_path: str, local_path: str) -> None:
        '''Download a single file from the Minio object storage

        Args:
         - bucket: Name of the bucket to download the file from
         - minio_path: Path of the object inside the Minio storage
         - local_path: Path for the downloaded local file
        '''
        try:
            for o in self.list_objects(bucket, '', True):
                if minio_path in o or re.search(minio_path, o):
                    self.client.fget_object(bucket, o, local_path)
        except S3Error as e:
            self.logger.error('Could not download file "{}" from bucket "{}"'.format(minio_path, bucket))
            if self.raise_errors:
                raise RuntimeError('Could not download file "{}" from bucket "{}"'.format(minio_path, bucket)) from e
        except ValueError as e:
            self.logger.error('Invalid input for downloading file from Minio (inputs: bucket={},minio_path={},local_path={})'.format(bucket,minio_path, local_path))
            if self.raise_errors:
                raise RuntimeError('Invalid input for downloading file from Minio (inputs: bucket={},minio_path={},local_path={})'.format(bucket,minio_path, local_path)) from e
        else:
            self.logger.info('Downloaded file "{}" from bucket "{}" to local location "{}"'.format(minio_path, bucket, local_path))


    @classmethod
    def download_directory(self, bucket:str, minio_path:str, local_path:str) -> None:
        '''Download an entire directory from the Minio object storage

        Args:
         - bucket: Name of the bucket to download the directory from
         - minio_path: Path of the object inside the Minio storage
         - local_path: Path for the downloaded local directory
        '''
        for obj in self.client.list_objects(bucket, prefix=minio_path, recursive=True):
            path_list = obj.object_name.split(os.path.sep)
            path_list[0] = local_path
            full_path = os.path.join(*path_list)
            self.download_file(bucket, obj.object_name, full_path)


    @classmethod
    def _read_credentials(self, credentials_path: str) -> dict|None:
        '''Read Minio credentials

        For INTERNAL USE ONLY!
        '''
        try:
            with open(os.path.join(credentials_path,'minio_credentials.json'),'r') as f:
                credentials = json.loads(f.read())
        except FileNotFoundError as e:
            self.logger.error('Could not find credentials file "minio_credentials.json" at location: "{}"'.format(credentials_path))
            if self.raise_errors:
                raise RuntimeError('Could not find credentials file "minio_credentials.json" at location: "{}"'.format(credentials_path)) from e
        except JSONDecodeError as e:
            self.logger.error('Could not decode credentials file "{}" (Invalid JSON)'.format(os.path.join(credentials_path,'minio_credentials.json')))
            if self.raise_errors:
                raise RuntimeError('Could not decode credentials file "{}" (Invalid JSON)'.format(os.path.join(credentials_path,'minio_credentials.json'))) from e
        else:
            return credentials


    @classmethod
    def _http_client(self, timeout: urllib3.Timeout=urllib3.Timeout.DEFAULT_TIMEOUT, retries: int=5) -> urllib3.PoolManager:
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
    print(minio_client.list_buckets())
    # minio_client.upload_directory(bucket_name, 'test_2', '../test_2')
    minio_client.download_directory('minio.python.api.test', 'configs', './test')

if __name__=="__main__":
    main()