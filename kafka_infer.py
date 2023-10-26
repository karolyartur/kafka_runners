import os
import argparse
import json
import logging
import time
import pymongo
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
parser.add_argument("-dbu", "--db_user", dest='db_user', type=str, help="User for DB", default=None)
parser.add_argument("-dbp", "--db_pass", dest='db_pass', type=str, help="Pass for DB", default=None)
parser.add_argument("-dbip", "--db_ip", dest='db_ip', type=str, help="IP of DB", default=None)
parser.add_argument("-dbn", "--db_name", dest='db_name', type=str, help="Name of DB", default=None)
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
            db_user = 'user',
            db_pass = '',
            db_ip = 'mongodb:27017',
            db_name = '',
            schema_path='../json_schemas',
            consumer_group_id = None,
            error_topic_name='error.log',
            loglevel = logging.WARN):
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

        self.mongo_client = pymongo.MongoClient("mongodb://{}:{}@{}/?authMechanism=DEFAULT&authSource={}".format(db_user,db_pass,db_ip,db_name))
        self.db = self.mongo_client[db_name]
        try:
            # Test DB connection
            self.db.list_collection_names()
        except pymongo.errors.ServerSelectionTimeoutError:
            self.logger.error('Timout while trying to list available DB collections. Connection to DB might be lost. Results will not be stored in DB!')
            self.db = None
        except pymongo.errors.OperationFailure as e:
            self.logger.error(e)
            self.db = None

        self.mrcnn = None


        self.service_info = self.construct_and_validate_service_info(self.schema_path, 'Mask-RCNN Inference', 'Makes predictions with Mask-RCNN models using CPU', self.inference_job_schema, self.inference_output_schema)

    def msg_to_command(self, msg):
        '''Convert incoming Kafka messages to a command for the inference
        '''
        # Validate incoming messages (is it valid JSON? Is it valid, according to the schema?)
        try:
            msgs_json = []
            for m in msg:
                msg_json = m
                if isinstance(msg_json, str):
                    msg_json = json.loads(msg_json)
                resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'inference_job.schema.json', referrer=None)
                validate(instance=msg_json, schema=self.inference_job_schema, resolver=resolver)
                msgs_json.append(msg_json)
        except JSONDecodeError:
            self.logger.warning('Messages "{}" could not be decoded (invalid JSON)\nIgnoring messages'.format(msg))
        except TypeError as e:
            self.logger.warning('Messages "{}" could not be decoded (invalid input type for JSON decoding). {}\nIgnoring messages'.format(msg, e))
        except ValidationError:
            self.logger.warning('Messages "{}" failed JSON schema validation (used schema: {})\nIgnoring messages'.format(msg, os.path.join(self.schema_path,'inference_job.schema.json')))
        else:
            # If the messages are valid:

            #Set defaults of message
            max_dets = 100
            put_to_db = False
            model_local_path = ''

            # Get data from Minio storage
            # Download model weights

            # Validate, that all messages use the same model

            if len(set([m['modelWeightsFilePath'] for m in msgs_json])) == 1:
                weights_file_path = os.path.normpath(msgs_json[0]['modelWeightsFilePath']).split(os.path.sep)
                weights_file_bucket = weights_file_path[0]
                weights_file_name = weights_file_path[-1]
                weights_file_path = os.path.sep.join(weights_file_path[1:])
                model_local_path = os.path.join('models',weights_file_name)

                self.logger.info('Downloading model weights file from MinIO path "{}" and saving it at local path "{}"'.format(weights_file_path, model_local_path))

                try:
                    if not os.path.exists('models'):
                        os.mkdir('models')
                    if not weights_file_name in os.listdir('models'):
                        self.client.download_file(weights_file_bucket, weights_file_path, os.path.join('models',weights_file_name))
                except RuntimeError:
                    self.logger.warning('Could not locate and/or download model weights from "{}" bucket and location "{}". Inference will NOT start!'.format(weights_file_bucket,weights_file_path))
                    return None

                self.logger.info('Finished downloading model weights file from MinIO path "{}" and saving it at local path "{}"'.format(weights_file_path, model_local_path))
            else:
                self.logger.warning('Messages in batch "{}" want to use different models! No predictions will be made.'.format(msgs_json))
                return None

            # Construct and return command if data is ready

            if len(set([m['maxDets'] for m in msgs_json])) == 1:
                max_dets = msgs_json[0]['maxDets']
            else:
                self.logger.warning('Messages in batch "{}" want to use different maxDets! Default value will be used:{}'.format(msgs_json, max_dets))
            if len(set([m['puttoDB'] for m in msgs_json])) == 1:
                put_to_db = msgs_json[0]['puttoDB']
            else:
                self.logger.warning('Messages in batch "{}" want to use different puttoDB! Default value will be used:{}'.format(msgs_json, put_to_db))

            # Get img paths and names
            img_paths = [os.path.normpath(m['imagePath']) for m in msgs_json]
            output_paths = [os.path.normpath(m['outputLocation']) for m in msgs_json]


            img_names = [os.path.splitext(img_path.split(os.path.sep)[-1])[0] for img_path in img_paths]

            # Do inference
            start_time = time.time()
            self.logger.info('Starting inference ...')
            if self.mrcnn is None:
                self.mrcnn = MRCNNInference(self.client._s3, model_local_path, max_dets=max_dets)
            else:
                self.mrcnn.load_model_weights(model_local_path,max_dets)
            
            boxes, masks = self.mrcnn.predict(img_paths)

            boxes_paths = [os.path.join(output_path,'{}_boxes.json'.format(img_name)) for output_path,img_name in zip(output_paths, img_names)]
            masks_paths = [os.path.join(output_path,'{}_masks.npz'.format(img_name)) for output_path,img_name in zip(output_paths, img_names)]

            # Save results
            for i,(boxes_path, masks_path, img_name) in enumerate(zip(boxes_paths,masks_paths, img_names)):
                self.logger.info('Saving results from image {} to MinIO paths "{}" and "{}" using s3'.format(i, boxes_path, masks_path))
                with self.client._s3.open(boxes_path, 'w') as f:
                    f.write(json.dumps(boxes[i]))
                with self.client._s3.open(masks_path, 'wb') as f:
                    np.savez_compressed(f, masks=masks[i])

                if put_to_db and self.db is not None:
                    self.logger.info('Saving results from image {} to DB'.format(i))
                    collection = self.db[os.path.splitext(weights_file_name)[0]]
                    try:
                        collection.insert_one({'_id':img_name, 'boxes_path':boxes_path, 'masks_path':masks_path, 'predictions':boxes[i]})
                    except pymongo.errors.DuplicateKeyError:
                        pass
            self.logger.info('Finished saving results')
                
            end_time = time.time()

            self.elapsed_time = end_time-start_time
            self.boxes = boxes[0]
            cmd = ['echo','']
            self.logger.info('Command to be executed: {}'.format(cmd))
            return cmd

    def make_response(self, in_msg, elapsed_time):
        '''Construct a response after the inference is completed
        '''
        try:
            for msg_json in in_msg:
                if isinstance(msg_json, str):
                    msg_json = json.loads(msg_json)
                resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'inference_job.schema.json', referrer=None)
                validate(instance=msg_json, schema=self.inference_job_schema, resolver=resolver)
        except TypeError as e:
            self.logger.warning('Messages "{}" could not be decoded (invalid input type for JSON decoding). {}\nIgnoring messages'.format(in_msg, e))
        except JSONDecodeError:
            self.logger.warning('Messages "{}" could not be decoded (invalid JSON)\nIgnoring messages'.format(in_msg))
        except ValidationError:
            self.logger.warning('Messages "{}" failed JSON schema validation (used schema: {})\nIgnoring messages'.format(in_msg, os.path.join(self.schema_path,'inference_job.schema.json')))
        else:
            response = {}
            response['inferenceJob'] = in_msg[0]
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
    mrcnn_trainer = KafkaInfer(
        args.in_topic_name,
        args.out_topic_name,
        args.bootstrap_servers,
        db_user = args.db_user,
        db_pass = args.db_pass,
        db_ip = args.db_ip,
        db_name = args.db_name,
        consumer_group_id = args.consumer_group_id,
        error_topic_name = args.error_topic_name,
        loglevel = logging.DEBUG if args.debug else logging.WARN)
    mrcnn_trainer.start_listening()

if __name__=='__main__':
    main()
