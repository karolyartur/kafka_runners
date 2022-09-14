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
    def __init__(self, credentials_path='../credentials', logger=None):
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
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)
            self.logger.info('Created bucket "{}"'.format(bucket))
        else:
            self.logger.info('Bucket "{}" already exists!'.format(bucket))

    def upload_file(self, bucket, minio_path, local_path):
        try:
            self.client.fput_object(bucket, minio_path, local_path)
        except S3Error as e:
            self.logger.error('Could not upload file "{}" to bucket "{}"'.format(local_path, bucket))
            raise RuntimeError('Could not upload file "{}" to bucket "{}"'.format(local_path, bucket)) from e
        else:
            self.logger.info('Uploaded local file "{}" to bucket "{}" to location "{}"'.format(local_path, bucket, minio_path))

    def upload_directory(self, bucket, minio_path, local_path):
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
        for obj in self.client.list_objects(bucket, prefix=minio_path, recursive=True):
            self.download_file(bucket, obj.object_name, obj.object_name.replace(minio_path,local_path))


    def _read_credentials(self, credentials_path):
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