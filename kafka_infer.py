import os
import argparse
import json
import logging
import time
import numpy as np
from kafka_runner import KafkaRunner
from minio_client import MinioClient
from mrcnn_inference import MRCNNInference
from jsonschema import validate, RefResolver
from jsonschema.exceptions import ValidationError
from json.decoder import JSONDecodeError

parser = argparse.ArgumentParser(description="Kafka Infer")
parser.add_argument("in_topic_name", type=str,help="in_topic name")
parser.add_argument("out_topic_name", type=str,help="out_topic name")
parser.add_argument("bootstrap_servers", type=str, nargs='+', help="list of bootstrap servers")
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
    def __init__(self, in_topic_name, out_topic_name, bootstrap_servers, schema_path='../json_schemas', consumer_group_id = None, error_topic_name='error.log', loglevel = logging.WARN):
        # Init base-class
        try:
            super().__init__(in_topic_name, out_topic_name, bootstrap_servers, consumer_group_id=consumer_group_id, error_topic_name=error_topic_name, loglevel=loglevel)
        except RuntimeError:
            exit(0)

        # Connect to Minio object storage
        try:
            self.client = MinioClient(logger = self.logger)
        except (RuntimeError,ValueError):
            exit(0)

        # Load JSON schemas
        self.schema_path = schema_path
        try:
            schema_path = os.path.join(self.schema_path,'inference_job.schema.json')
            with open(schema_path, 'r') as f:
                self.inference_job_schema = json.loads(f.read())
        except FileNotFoundError:
            self.logger.warning('JSON schema for Kafka message could not be found at path: "{}"\nIncoming messages will NOT be validated!'.format(schema_path))
            self.inference_job_schema = {}
        except JSONDecodeError:
            self.logger.warning('JSON schema for Kafka message at path: "{}" could not be decoded (invalid JSON)\nIncoming messages will NOT be validated!'.format(schema_path))
            self.inference_job_schema = {}

        try:
            schema_path = os.path.join(self.schema_path,'inference_output.schema.json')
            with open(schema_path, 'r') as f:
                self.inference_output_schema = json.loads(f.read())
        except FileNotFoundError:
            self.logger.warning('JSON schema for Kafka message could not be found at path: "{}"\nOutgoing messages will NOT be validated!'.format(schema_path))
            self.inference_output_schema = {}
        except JSONDecodeError:
            self.logger.warning('JSON schema for Kafka message at path: "{}" could not be decoded (invalid JSON)\nOutgoing messages will NOT be validated!'.format(schema_path))
            self.inference_output_schema = {}

        self.service_info = self.construct_and_validate_service_info(self.schema_path, 'Mask-RCNN Inference', 'Makes predictions with Mask-RCNN models using CPU', self.inference_job_schema, self.inference_output_schema)

    def msg_to_command(self, msg):
        '''Convert incoming Kafka messages to a command for the inference
        '''
        # Validate incoming message (is it valid JSON? Is it valid, according to the schema?)
        try:
            msg_json = msg.value
            if isinstance(msg_json, str):
                msg_json = json.loads(msg_json)
            resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'inference_job.schema.json', referrer=None)
            validate(instance=msg_json, schema=self.inference_job_schema, resolver=resolver)
        except JSONDecodeError:
            self.logger.warning('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(msg.value))
        except TypeError as e:
            self.logger.warning('Message "{}" could not be decoded (invalid input type for JSON decoding). {}\nIgnoring message'.format(msg.value, e))
        except ValidationError:
            self.logger.warning('Message "{}" failed JSON schema validation (used schema: {})\nIgnoring message'.format(msg.value, os.path.join(self.schema_path,'inference_job.schema.json')))
        else:
            # If the message is valid:

            #Set defaults of message
            max_dets = 100

            # Get data from Minio storage
            # Download model weights
            weights_file_path = os.path.normpath(msg_json['modelWeightsFilePath']).split(os.path.sep)
            weights_file_bucket = weights_file_path[0]
            weights_file_name = weights_file_path[-1]
            weights_file_path = os.path.sep.join(weights_file_path[1:])
            model_local_path = os.path.join('models',weights_file_name)

            try:
                if not os.path.exists('models'):
                    os.mkdir('models')
                if not weights_file_name in os.listdir('models'):
                    self.client.download_file(weights_file_bucket, weights_file_path, os.path.join('models',weights_file_name))
            except RuntimeError:
                self.logger.warning('Could not locate and/or download model weights from "{}" bucket and location "{}". Inference will NOT start!'.format(weights_file_bucket,weights_file_path))
                return None

            # Construct and return command if data is ready
            if 'maxDets' in msg_json:
                max_dets = msg_json['maxDets']

            # Get img path and name
            img_path = os.path.normpath(msg_json['imagePath'])
            output_path = os.path.normpath(msg_json['outputLocation'])


            img_name = os.path.splitext(img_path.split(os.path.sep)[-1])[0]

            # Do inference
            start_time = time.time()
            mrcnn = MRCNNInference(self.client._s3, model_local_path, max_dets=max_dets)
            boxes, masks = mrcnn.predict(img_path)

            # Save results
            with self.client._s3.open(os.path.join(output_path,'{}_boxes.json'.format(img_name)), 'w') as f:
                f.write(json.dumps(boxes))
            with self.client._s3.open(os.path.join(output_path,'{}_masks.npz'.format(img_name)), 'wb') as f:
                np.savez_compressed(f, masks=masks)
            end_time = time.time()

            self.elapsed_time = end_time-start_time
            self.boxes = boxes
            cmd = ['echo','']
            self.logger.info('Command to be executed: {}'.format(cmd))
            return cmd

    def make_response(self, in_msg, elapsed_time):
        '''Construct a response after the inference is completed
        '''
        try:
            msg_json = in_msg
            if isinstance(msg_json, str):
                msg_json = json.loads(msg_json)
            resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'inference_job.schema.json', referrer=None)
            validate(instance=msg_json, schema=self.inference_job_schema, resolver=resolver)
        except TypeError as e:
            self.logger.warning('Message "{}" could not be decoded (invalid input type for JSON decoding). {}\nIgnoring message'.format(in_msg, e))
        except JSONDecodeError:
            self.logger.warning('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(in_msg))
        except ValidationError:
            self.logger.warning('Message "{}" failed JSON schema validation (used schema: {})\nIgnoring message'.format(in_msg, os.path.join(self.schema_path,'inference_job.schema.json')))
        else:
            response = {}
            response['inferenceJob'] = msg_json
            response['timestamp'] = time.time()
            response['elapsedTime'] = self.elapsed_time
            response['predictions'] = self.boxes
            
            try:
                resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'inference_output.schema.json', referrer=None)
                validate(instance=response, schema=self.inference_output_schema, resolver=resolver)
            except ValidationError:
                self.logger.warning('Response "{}" failed JSON schema validation (used schema: {})\nIgnoring response'.format(response, os.path.join(self.schema_path,'inference_output.schema.json')))
            else:
                return response

def main():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    args = parser.parse_args()
    mrcnn_trainer = KafkaInfer(args.in_topic_name, args.out_topic_name, args.bootstrap_servers, consumer_group_id=args.consumer_group_id, error_topic_name=args.error_topic_name, loglevel=logging.DEBUG if args.debug else logging.WARN)
    mrcnn_trainer.start_listening()

if __name__=='__main__':
    main()