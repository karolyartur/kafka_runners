import os
import argparse
import json
import logging
import time
from kafka_runner import KafkaRunner
from minio_client import MinioClient
from jsonschema import validate, RefResolver
from jsonschema.exceptions import ValidationError
from json.decoder import JSONDecodeError

parser = argparse.ArgumentParser(description="Kafka Infer")
parser.add_argument("in_topic_name", type=str,help="in_topic name")
parser.add_argument("out_topic_name", type=str,help="out_topic name")
parser.add_argument("bootstrap_servers", type=str, nargs='+', help="list of bootstrap servers")
parser.add_argument("-w", "--weights_file", dest='weights_file', type=str, help="name of the weights file to use", default=None)
parser.add_argument("-md", "--max_dets", dest='max_dets', type=int, help="maximum number of detected objects", default=300)
parser.add_argument("-o", "--output_location", dest='output_location', type=str, help="where to save outputs in MinIO", default='mushroom.monitor/inference/real')
parser.add_argument("-db","--put_to_db",dest='put_to_db', action='store_true', help="store inference output in MongoDB", default=False)
parser.add_argument("-g", "--group_id", dest='consumer_group_id', type=str, help="consumer group id", default=None)
parser.add_argument("-e","--error_topic_name",dest='error_topic_name', type=str, help="name of the Kafka topic to log errors to", default='error.log')
parser.add_argument("-d","--debug",dest='debug', action='store_true', help="run the script in debug mode", default=False)


class KafkaInfer(KafkaRunner):
    '''Kafka Inference for DL models

    Incoming Kafka messages are validated against the inference_job.schema.json and outgoing messages are validated against the inference_output.schema.json

    Args:
     - in_topic_name (str): Name of Kafka topic from which incoming messages will be read
     - out_topic_name (str): Name of Kafka topic where outgoing messages will be sent to
     - bootstrap_servers (list of stings): Address of Kafka bootstrap servers
    Keyword args:
     - schema_path (str): Path to the folder containing the json schemas (default is '../json_schemas')
     - consumer_group_id (str): ID of the Kafka consumer group for the consumer of the Kafka Runner (default is None meaning no consumer group is used)
     - error_topic_name (str): Name of the Kafka topic to send error logs to (default is 'error.log')
     - loglevel (logging.DEBUG/WARN/...): Logging level (default is logging.WARN meaning warnings and higher level logs will be reported)
    '''
    def __init__(self, 
            in_topic_name,
            out_topic_name,
            bootstrap_servers,
            weights_file = None,
            max_dets = 300,
            output_location = 'mushroom.monitor/inference/real',
            put_to_db = False,
            schema_path='../json_schemas',
            consumer_group_id = None,
            error_topic_name='error.log',
            loglevel = logging.WARN):
        # Init base-class
        try:
            super().__init__(in_topic_name, out_topic_name, bootstrap_servers, consumer_group_id=consumer_group_id, error_topic_name=error_topic_name, loglevel=loglevel)
        except RuntimeError:
            exit(0)

        try:
            self.client = MinioClient(logger = self.logger)
        except (RuntimeError,ValueError):
            exit(0)


        # Load JSON schema
        self.schema_path = schema_path
        try:
            schema_path = os.path.join(self.schema_path,'inference_job.schema.json')
            with open(schema_path, 'r') as f:
                self.inference_job_schema = json.loads(f.read())
        except FileNotFoundError:
            self.logger.warning('JSON schema for Kafka message could not be found at path: "{}"\nOutgoing messages will NOT be validated!'.format(schema_path))
            self.inference_job_schema = {}
        except JSONDecodeError:
            self.logger.warning('JSON schema for Kafka message at path: "{}" could not be decoded (invalid JSON)\nOutgoing messages will NOT be validated!'.format(schema_path))
            self.inference_job_schema = {}


        weights_bucket = 'mushroom.monitor'
        weights_path = 'models/mrcnn/weights/'
        if weights_file:
            self.weights_file_path = weights_bucket + '/' + weights_path + weights_file
        else:
            weights_files = []
            try:
                weights_files = sorted(self.client.list_objects(weights_bucket,weights_path))
            except RuntimeError:
                pass
            if weights_files:
                weights_file = weights_files[-1]
                self.weights_file_path = weights_bucket + '/' + weights_path + weights_file
            else:
                self.logger.error('No weights file specified and none found in MinIO! Exiting.')
                exit(0)

        self.max_dets = max_dets
        self.output_location = output_location + '/' + os.path.splitext(weights_file)[0]
        self.put_to_db = put_to_db

        self.service_info = self.construct_and_validate_service_info(self.schema_path, 'MinIO trigger for automatic inference', 'Requests inference whenever a new image is available in MinIO', '', self.inference_job_schema)

    def msg_to_command(self, msg):
        '''Convert incoming Kafka messages to a command for the inference
        '''
        # Incoming messages are MinIO events and are not validated
        try:
            msg_json = msg.value
            if isinstance(msg_json, str):
                msg_json = json.loads(msg_json)
        except JSONDecodeError:
            self.logger.warning('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(msg.value))
        except TypeError as e:
            self.logger.warning('Message "{}" could not be decoded (invalid input type for JSON decoding). {}\nIgnoring message'.format(msg.value, e))
        else:
            # If the message is valid:

            start_time = time.time()
            img_path = ''
            if 'EventName' in msg_json and 'Key' in msg_json:
                if msg_json['EventName'] == 's3:ObjectCreated:Put' and 'fungi2rawvol1/data/img' in msg_json['Key']:
                    img_path = msg_json['Key']
            end_time = time.time()

            self.elapsed_time = end_time-start_time
            self.img_path = img_path
            cmd = ['echo','']
            self.logger.info('Command to be executed: {}'.format(cmd))
            return cmd

    def make_response(self, in_msg, elapsed_time):
        '''Construct a response after the inference is completed
        '''
        if self.img_path:
            response = {}
            response['jobId'] = 0
            response['timestamp'] = time.time()
            response['modelWeightsFilePath'] = self.weights_file_path
            response['imagePath'] = self.img_path
            response['maxDets'] = self.max_dets
            response['outputLocation'] = self.output_location
            response['puttoDB'] = self.put_to_db
            
            try:
                resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'inference_job.schema.json', referrer=None)
                validate(instance=response, schema=self.inference_job_schema, resolver=resolver)
            except ValidationError:
                self.logger.warning('Response "{}" failed JSON schema validation (used schema: {})\nIgnoring response'.format(response, os.path.join(self.schema_path,'inference_job.schema.json')))
            else:
                return response

def main():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    args = parser.parse_args()
    mrcnn_trainer = KafkaInfer(
        args.in_topic_name,
        args.out_topic_name,
        args.bootstrap_servers,
        weights_file = args.weights_file,
        max_dets = args.max_dets,
        output_location = args.output_location,
        put_to_db = args.put_to_db,
        consumer_group_id = args.consumer_group_id,
        error_topic_name = args.error_topic_name,
        loglevel = logging.DEBUG if args.debug else logging.WARN)
    mrcnn_trainer.start_listening()

if __name__=='__main__':
    main()
