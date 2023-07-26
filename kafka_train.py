import os
import argparse
import json
import logging
import time
import yaml
import shutil
from kafka_runner import KafkaRunner
from minio_client import MinioClient
from train_mrcnn import MRCNNTrainer
from jsonschema import validate, RefResolver
from jsonschema.exceptions import ValidationError
from json.decoder import JSONDecodeError

parser = argparse.ArgumentParser(description="Kafka Trainer")
parser.add_argument("in_topic_name", type=str,help="in_topic name")
parser.add_argument("out_topic_name", type=str,help="out_topic name")
parser.add_argument("bootstrap_servers", type=str, nargs='+', help="list of bootstrap servers")
parser.add_argument("-g", "--group_id", dest='consumer_group_id', type=str, help="consumer group id", default=None)
parser.add_argument("-e","--error_topic_name",dest='error_topic_name', type=str, help="name of the Kafka topic to log errors to", default='error.log')
parser.add_argument("-d","--debug",dest='debug', action='store_true', help="run the script in debug mode", default=False)


class KafkaTrainer(KafkaRunner):
    '''Kafka Trainer for training Mask-RCNN models

    Incoming Kafka messages are validated against the training_job.schema.json and outgoing messages are validated against the training_output.schema.json

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

        self.temp_training_output = os.path.join(os.path.dirname(os.path.abspath(__file__)),'training_output')

        # Load JSON schemas
        self.schema_path = schema_path
        try:
            schema_path = os.path.join(self.schema_path,'training_job.schema.json')
            with open(schema_path, 'r') as f:
                self.training_job_schema = json.loads(f.read())
        except FileNotFoundError:
            self.logger.warn('JSON schema for Kafka message could not be found at path: "{}"\nIncoming messages will NOT be validated!'.format(schema_path))
            self.training_job_schema = {}
        except JSONDecodeError:
            self.logger.warn('JSON schema for Kafka message at path: "{}" could not be decoded (invalid JSON)\nIncoming messages will NOT be validated!'.format(schema_path))
            self.training_job_schema = {}

        try:
            schema_path = os.path.join(self.schema_path,'training_output.schema.json')
            with open(schema_path, 'r') as f:
                self.training_output_schema = json.loads(f.read())
        except FileNotFoundError:
            self.logger.warn('JSON schema for Kafka message could not be found at path: "{}"\nOutgoing messages will NOT be validated!'.format(schema_path))
            self.training_output_schema = {}
        except JSONDecodeError:
            self.logger.warn('JSON schema for Kafka message at path: "{}" could not be decoded (invalid JSON)\nOutgoing messages will NOT be validated!'.format(schema_path))
            self.training_output_schema = {}

    def msg_to_command(self, msg):
        '''Convert incoming Kafka messages to a command for the training
        '''
        # Validate incoming message (is it valid JSON? Is it valid, according to the schema?)
        try:
            msg_json = msg.value
            if isinstance(msg_json, str):
                msg_json = json.loads(msg_json)
            resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'training_job.schema.json', referrer=None)
            validate(instance=msg_json, schema=self.training_job_schema, resolver=resolver)
        except JSONDecodeError:
            self.logger.warn('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(msg.value))
        except TypeError as e:
            self.logger.warn('Message "{}" could not be decoded (invalid input type for JSON decoding). {}\nIgnoring message'.format(msg.value, e))
        except ValidationError:
            self.logger.warn('Message "{}" failed JSON schema validation (used schema: {})\nIgnoring message'.format(msg.value, os.path.join(self.schema_path,'training_job.schema.json')))
        else:
            # If the message is valid:

            #Set defaults of message
            model_name = 'model'
            class_info = None
            epochs = 1
            init_with = 'coco'
            layers = 'heads'

            # Get data from Minio storage
            # Download training data
            try:
                train_data_path = os.path.normpath(msg_json['trainDataPath']).split(os.path.sep)
                train_data_bucket = train_data_path[0]
                train_data_name = train_data_path[-1]
                train_data_path = os.path.sep.join(train_data_path[1:])
                if not os.path.exists('train'):
                    os.mkdir('train')
                if not train_data_name in os.listdir('train'):
                    self.client.download_directory(train_data_bucket, train_data_path, os.path.join('train',train_data_name))
                train = os.path.join('train', train_data_name)
            except RuntimeError:
                self.logger.warning('Could not locate and/or download training data from "{}" bucket and location "{}". Training will NOT start!'.format(train_data_bucket,train_data_path))
                return None
            
            # Download validation data
            try:
                valid_data_path = os.path.normpath(msg_json['validDataPath']).split(os.path.sep)
                valid_data_bucket = valid_data_path[0]
                valid_data_name = valid_data_path[-1]
                valid_data_path = os.path.sep.join(valid_data_path[1:])
                if not os.path.exists('valid'):
                    os.mkdir('valid')
                if not valid_data_name in os.listdir('valid'):
                    self.client.download_directory(valid_data_bucket, valid_data_path, os.path.join('valid',valid_data_name))
                valid = os.path.join('valid', valid_data_name)
            except RuntimeError:
                self.logger.warning('Could not locate and/or download validation data from "{}" bucket and location "{}". Training will NOT start!'.format(valid_data_bucket,valid_data_path))
                return None
            
            # Download config file
            try:
                config_file_path = os.path.normpath(msg_json['configFilePath']).split(os.path.sep)
                config_file_bucket = config_file_path[0]
                config_file_name = config_file_path[-1]
                config_file_path = os.path.sep.join(config_file_path[1:])
                if not os.path.exists('config'):
                    os.mkdir('config')
                if not config_file_name in os.listdir('config'):
                    self.client.download_file(config_file_bucket, config_file_path, os.path.join('config',config_file_name))
                config = os.path.join('config', config_file_name)
            except RuntimeError:
                self.logger.warning('Could not locate and/or download configuration file from "{}" bucket and location "{}". Training will NOT start!'.format(config_file_bucket,config_file_path))
                return None

            # Download class info
            if 'classInfoFilePath' in msg_json:
                if msg_json['classInfoFilePath'] == "":
                    class_info = None
                else:
                    try:
                        class_info_file_path = os.path.normpath(msg_json['classInfoFilePath']).split(os.path.sep)
                        class_info_file_bucket = class_info_file_path[0]
                        class_info_file_name = class_info_file_path[-1]
                        class_info_file_path = os.path.sep.join(class_info_file_path[1:])
                        if not os.path.exists('config'):
                            os.mkdir('config')
                        if not class_info_file_name in os.listdir('config'):
                            self.client.download_file(class_info_file_bucket, class_info_file_path, os.path.join('config',class_info_file_name))
                        with open(os.path.join('config', class_info_file_name),'r') as f:
                            class_info = yaml.safe_load(f)
                            if not type(class_info) == list and not class_info is None:
                                self.logger.warning('Class info MUST be of type "list". Training will NOT start!'.format(class_info_file_bucket,class_info_file_path))
                                return None
                    except RuntimeError:
                        self.logger.warning('Could not locate and/or download class info file from "{}" bucket and location "{}". Training will NOT start!'.format(class_info_file_bucket,class_info_file_path))
                        return None
                    except FileNotFoundError:
                        self.logger.warning('Could not find the class info file file at {}. Training will NOT start!'.format(os.path.join('config', class_info_file_name)))
                        return None
                    except yaml.YAMLError:
                        self.logger.warning('Could not load the class info file at {}. Invalid YAML! Training will NOT start!'.format(os.path.join('config', class_info_file_name)))
                        return None
            else:
                class_info = None

            # Construct and return command if data is ready
            if 'modelName' in msg_json:
                model_name = msg_json['modelName']
            if 'epochs' in msg_json:
                epochs = msg_json['epochs']
            if 'initWeights' in msg_json:
                init_with = msg_json['initWeights']
                if not init_with in ['coco', 'imagenet', 'last']:
                    self.logger.warning('Initial weights MUST either be "coco", "imagenet" or "last", but  {} was provided. Using "coco" instead'.format(init_with))
                    init_with = 'coco'
            if 'layers' in msg_json:
                layers = msg_json['layers']

            output_path = os.path.split(self.temp_training_output)[-1]
            if not os.path.exists(output_path):
                os.mkdir(output_path)

            start_time = time.time()
            trainer = MRCNNTrainer(config, train, valid, cls_info=class_info, model_name=model_name, output_path=output_path,logger=self.logger)
            trainer.train(init_with=init_with, epochs=epochs, layers=layers)
            end_time = time.time()
            self.elapsed_time = end_time-start_time
            cmd = ['echo','']
            self.logger.info('Command to be executed: {}'.format(cmd))
            return cmd

    def make_response(self, in_msg, elapsed_time):
        '''Construct a response after the training is completed
        '''
        try:
            msg_json = in_msg
            if isinstance(msg_json, str):
                msg_json = json.loads(msg_json)
            resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'training_job.schema.json', referrer=None)
            validate(instance=msg_json, schema=self.training_job_schema, resolver=resolver)
        except TypeError as e:
            self.logger.warn('Message "{}" could not be decoded (invalid input type for JSON decoding). {}\nIgnoring message'.format(in_msg, e))
        except JSONDecodeError:
            self.logger.warn('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(in_msg))
        except ValidationError:
            self.logger.warn('Message "{}" failed JSON schema validation (used schema: {})\nIgnoring message'.format(in_msg, os.path.join(self.schema_path,'training_job.schema.json')))
        else:
            response = {}
            response['trainingJob'] = msg_json
            response['timestamp'] = time.time()
            response['elapsedTime'] = self.elapsed_time
            
            if 'outputLocation' in msg_json:
                output_location = os.path.normpath(msg_json['outputLocation']).split(os.path.sep)
                output_bucket = output_location[0]
                output_minio_path = os.path.sep.join(output_location[1:])
                try:
                    self.client.create_bucket(output_bucket)
                    self.client.upload_directory(output_bucket, output_minio_path, self.temp_training_output)
                except RuntimeError:
                    self.logger.error('Could not upload training output to Minio storage! (target bucket:{}, target Minio path: {}, local path:{})'.format(output_bucket,output_minio_path,self.temp_training_output))
                else:
                    self.logger.info('Removing temporary local copy of training output')
                    for element in os.listdir(self.temp_training_output):
                        if os.path.isfile(os.path.join(self.temp_training_output, element)):
                            os.remove(os.path.join(self.temp_training_output, element))
                        elif os.path.isdir(os.path.join(self.temp_training_output, element)):
                            shutil.rmtree(os.path.join(self.temp_training_output, element), ignore_errors=True)
            else:
                self.logger.warn('No output location specified in incoming message! Training output will only be saved locally!')
            try:
                resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'training_output.schema.json', referrer=None)
                validate(instance=response, schema=self.training_output_schema, resolver=resolver)
            except ValidationError:
                self.logger.warn('Response "{}" failed JSON schema validation (used schema: {})\nIgnoring response'.format(response, os.path.join(self.schema_path,'training_output.schema.json')))
            else:
                return response

def main():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    args = parser.parse_args()
    mrcnn_trainer = KafkaTrainer(args.in_topic_name, args.out_topic_name, args.bootstrap_servers, consumer_group_id=args.consumer_group_id, error_topic_name=args.error_topic_name, loglevel=logging.DEBUG if args.debug else logging.WARN)
    mrcnn_trainer.start_listening()

if __name__=='__main__':
    main()