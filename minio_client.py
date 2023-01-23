import os
import re
import json
import logging
import argparse
import urllib3
import inspect
import minio.helpers
from minio import Minio
from urllib3.exceptions import MaxRetryError
from urllib.parse import urlparse, ParseResult
from minio.error import InvalidResponseError, S3Error, ServerError
from json.decoder import JSONDecodeError


class ClassPropertyDescriptor():

    def __init__(self, fget, fset=None):
        self.fget = fget
        self.fset = fset

    def __get__(self, instance, owner=None):
        if owner is None:
            owner = type(instance)
        return self.fget.__get__(instance, owner)()

    def __set__(self, instance, value):
        if not self.fset:
            raise AttributeError("can't set attribute")
        if inspect.isclass(instance):
            type_ = instance
            instance = None
        else:
            type_ = type(instance)
        return self.fset.__get__(instance, type_)(value)

    def setter(self, func):
        if not isinstance(func, (classmethod, staticmethod)):
            func = classmethod(func)
        self.fset = func
        return self

def classproperty(func):
    if not isinstance(func, (classmethod, staticmethod)):
        func = classmethod(func)

    return ClassPropertyDescriptor(func)


class ClassPropertyMetaClass(type):
    def __setattr__(cls, key, value):
        obj = None
        if key in cls.__dict__:
            obj = cls.__dict__.get(key)
        if obj and type(obj) is ClassPropertyDescriptor:
            return obj.__set__(cls, value)

        return super().__setattr__(key, value)

parser = argparse.ArgumentParser(description="Minio client")
parser.add_argument("credentials_path", type=str ,help="path to folder containing credentials")

class MinioClient(metaclass= ClassPropertyMetaClass):
    '''Minio client

    Class for convenient interaction with the Minio object storage

    Args:
     - credentials_path (str): Path to the folder containing the credentials files (default is '../credentials')
     - logger (logging.logger object): A logger can be passed, if the Minio client is used inside a class that has its own logger already (default is None which means a new logger will be created)
    '''
    logging.basicConfig()
    _logger = logging.getLogger(__name__ + '.MinioClient')
    _logger.setLevel(logging.DEBUG)
    _client = None
    _debug = True
    _raise_errors = True

    @classproperty
    def logger(self):
        return self._logger

    @logger.setter
    def logger(self, value):
        if isinstance(value, logging.Logger):
            self._logger = value
        else:
            raise TypeError('"logger" must be of type "logging.Logger"')

    @classproperty
    def client(self):
        return self._client

    @client.setter
    def client(self, value):
        if isinstance(value, Minio):
            self._client = value
        else:
            raise TypeError('"client" must be an instance of the "Minio" class')

    @classproperty
    def debug(self):
        return self._debug
    
    @debug.setter
    def debug(self, value):
        if isinstance(value, bool):
            self._debug = value
            if self.logger:
                if value:
                    self.logger.setLevel(logging.DEBUG)
                else:
                    self.logger.setLevel(logging.WARN)
        else:
            raise TypeError('"debug" must be a boolean')

    @classproperty
    def raise_errors(self):
        return self._raise_errors

    @raise_errors.setter
    def raise_errors(self, value):
        if isinstance(value, bool):
            self._raise_errors = value
        else:
            raise TypeError('"raise_errors" must be a boolean')


    def __init__(self, credentials_path='../credentials', logger=None):
        '''Constructor for Minio clients
        '''
        self.connect(credentials_path, logger)

    @classmethod
    def connect(self, credentials_path = '../credentials', logger = None):
        if logger:
            self.logger = logger
        if self.debug:
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
                if self.raise_errors:
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
    def list_buckets(self):
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

    @classmethod
    def list_objects(self, bucket, minio_path, recursive=False, start_after=None):
        '''List objects in the Minio object storage

        Args:
         - bucket (str): Name of the bucket in which the objects are
         - minio_path (str): Path for the folder (in the MinIO object storage) in which the objects are
         - recursive (bool): List objects in subdirectories
         - start_after (str): List objects after this key name
        '''
        return [o.object_name for o in self.client.list_objects(bucket, minio_path, recursive, start_after)]

    @classmethod
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
            if self.raise_errors:
                raise RuntimeError('Could not upload file "{}" to bucket "{}"'.format(local_path, bucket)) from e
        else:
            self.logger.info('Uploaded local file "{}" to bucket "{}" to location "{}"'.format(local_path, bucket, minio_path))

    @classmethod
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

    @classmethod
    def download_file(self, bucket, minio_path, local_path):
        '''Download a single file from the Minio object storage

        Args:
         - bucket (str): Name of the bucket to download the file from
         - minio_path (str): Path of the object inside the Minio storage
         - local_path (str): Path for the downloaded local file
        '''
        try:
            for o in self.list_objects(bucket, '', True):
                if minio_path in o or re.search(minio_path, o):
                    self.client.fget_object(bucket, o, os.path.join(local_path, o))
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
    def download_directory(self, bucket, minio_path, local_path):
        '''Download an entire directory from the Minio object storage

        Args:
         - bucket (str): Name of the bucket to download the directory from
         - minio_path (str): Path of the object inside the Minio storage
         - local_path (str): Path for the downloaded local directory
        '''
        for obj in self.client.list_objects(bucket, prefix=minio_path, recursive=True):
            self.download_file(bucket, obj.object_name,local_path)


    @classmethod
    def _read_credentials(self, credentials_path):
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