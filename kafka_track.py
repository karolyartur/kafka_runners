import os
import argparse
import json
import logging
import time
import pymongo
import numpy as np
from kafka.errors import KafkaTimeoutError
from kafka_runner import KafkaRunner
from minio_client import MinioClient
from tracking_utils import Tracker
from jsonschema import validate, RefResolver
from jsonschema.exceptions import ValidationError
from json.decoder import JSONDecodeError

parser = argparse.ArgumentParser(description="Kafka Track")
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


class KafkaTrack(KafkaRunner):
    '''Tracking for mushrooms

    Incoming Kafka messages are validated against the mushroom_tracking_job.schema.json and outgoing messages are validated against the mushroom_tracking_output.schema.json

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
            schema_path = os.path.join(self.schema_path,'mushroom_tracking_job.schema.json')
            with open(schema_path, 'r') as f:
                self.mushroom_tracking_job_schema = json.loads(f.read())
        except FileNotFoundError:
            self.logger.warning('JSON schema for Kafka message could not be found at path: "{}"\nIncoming messages will NOT be validated!'.format(schema_path))
            self.mushroom_tracking_job_schema = {}
        except JSONDecodeError:
            self.logger.warning('JSON schema for Kafka message at path: "{}" could not be decoded (invalid JSON)\nIncoming messages will NOT be validated!'.format(schema_path))
            self.mushroom_tracking_job_schema = {}

        try:
            schema_path = os.path.join(self.schema_path,'mushroom_tracking_output.schema.json')
            with open(schema_path, 'r') as f:
                self.mushroom_tracking_output_schema = json.loads(f.read())
        except FileNotFoundError:
            self.logger.warning('JSON schema for Kafka message could not be found at path: "{}"\nOutgoing messages will NOT be validated!'.format(schema_path))
            self.mushroom_tracking_output_schema = {}
        except JSONDecodeError:
            self.logger.warning('JSON schema for Kafka message at path: "{}" could not be decoded (invalid JSON)\nOutgoing messages will NOT be validated!'.format(schema_path))
            self.mushroom_tracking_output_schema = {}

        self.mongo_client = pymongo.MongoClient("mongodb://{}:{}@{}/?authMechanism=DEFAULT&authSource={}".format(db_user,db_pass,db_ip,db_name))
        self.db = self.mongo_client[db_name]
        try:
            # Test DB connection
            self.db.list_collection_names()
        except pymongo.errors.ServerSelectionTimeoutError:
            self.logger.error('Timout while trying to list available DB collections. Connection to DB might be lost!')
            exit(0)
        except pymongo.errors.OperationFailure as e:
            self.logger.error(e)
            exit(0)


        self.service_info = self.construct_and_validate_service_info(self.schema_path, 'Mushroom tracking', 'Matches mushroom detections accross multiple consecutive images', self.mushroom_tracking_job_schema, self.mushroom_tracking_output_schema)

    def msg_to_command(self, msg):
        '''Convert incoming Kafka messages to a command for the mushroom tracking
        '''
        # Validate incoming message (is it valid JSON? Is it valid, according to the schema?)
        try:
            msg_json = msg.value
            if isinstance(msg_json, str):
                msg_json = json.loads(msg_json)
            resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'mushroom_tracking_job.schema.json', referrer=None)
            validate(instance=msg_json, schema=self.mushroom_tracking_job_schema, resolver=resolver)
        except JSONDecodeError:
            self.logger.warning('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(msg.value))
        except TypeError as e:
            self.logger.warning('Message "{}" could not be decoded (invalid input type for JSON decoding). {}\nIgnoring message'.format(msg.value, e))
        except ValidationError:
            self.logger.warning('Message "{}" failed JSON schema validation (used schema: {})\nIgnoring message'.format(msg.value, os.path.join(self.schema_path,'mushroom_tracking_job.schema.json')))
        else:
            # If the message is valid:

            #Set defaults of message
            iou_thrd = 0.5
            max_candidate_age = 2
            max_lost_age = 4
            RoI = None
            weight_file_name = 'mushroom_0000_maskrcnn_weights_2023_08_01_0038'

            # Construct and return command if data is ready
            if 'IoUThrd' in msg_json:
                iou_thrd = msg_json['IoUThrd']
            if 'maxCandidateAge' in msg_json:
                max_candidate_age = msg_json['maxCandidateAge']
            if 'maxLostAge' in msg_json:
                max_lost_age = msg_json['maxLostAge']
            if 'RoI' in msg_json:
                RoI = msg_json['RoI']
            if 'weightFileName' in msg_json:
                weight_file_name = msg_json['weightFileName']

            date_start = msg_json['dateStart']
            date_end = msg_json['dateEnd']

            bucket = 'mushroom.monitor'
            minio_path = os.path.join('tracking', weight_file_name, date_start+'_'+date_end+'.json')
            outputpath = os.path.join(bucket,minio_path)

            start_time = time.time()
            if not self.client.list_objects(bucket,minio_path):
                self.logger.info('No saved tracking result found in MinIO, performing tracking from detections in DB')
                myquery = {"_id": {"$gte":date_start, "$lt":date_end}}
                tracker = Tracker(iou_thrd, max_candidate_age, max_lost_age, filter=RoI)

                # Tracking
                tracking_result = []
                timestep = 0
                self.logger.info('Querying detections from DB and performing tracking')
                for x in self.db[weight_file_name].find(myquery, {"_id":1,"predictions":1}):
                    # Loop through all detections
                    detections = x['predictions']['boxes']
                    if timestep == 0:
                        for detection in detections:
                            tracker.add_candidate_instance(detection, x['_id'])
                    else:
                        tracker.match(detections,x['_id'])
                        tracking_result.append(tracker.export(x['_id']))
                    timestep += 1

                # Save results
                self.logger.info('Saving tracking results to MinIO')
                with self.client._s3.open(outputpath, 'w') as f:
                    f.write(json.dumps(tracking_result))
            else:
                self.logger.info('Found saved tracking results, loading file from MinIO')
                with self.client._s3.open(outputpath, 'r') as f:
                    tracking_result = json.loads(f.read())
                
            end_time = time.time()

            self.elapsed_time = end_time-start_time
            self.tracking_results = {}

            self.logger.info('Constructing reformatted (output compatible) tracking results')
            for snapshot in tracking_result:
                timestamp = snapshot['img_id']
                for tracked_instance in snapshot['tracked']:
                    if tracked_instance['id'] in self.tracking_results.keys():
                        self.tracking_results[tracked_instance['id']].append({'timestamp':timestamp, 'size':tracked_instance['size']})
                    else:
                        self.tracking_results[tracked_instance['id']] = [{'timestamp':timestamp, 'size':tracked_instance['size']}]

            cmd = ['echo','']
            self.logger.info('Command to be executed: {}'.format(cmd))
            return cmd

    def make_response(self, in_msg, elapsed_time):
        '''Construct a response after the tracking is completed
        '''
        try:
            msg_json = in_msg
            if isinstance(msg_json, str):
                msg_json = json.loads(msg_json)
            resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'mushroom_tracking_job.schema.json', referrer=None)
            validate(instance=msg_json, schema=self.mushroom_tracking_job_schema, resolver=resolver)
        except TypeError as e:
            self.logger.warning('Message "{}" could not be decoded (invalid input type for JSON decoding). {}\nIgnoring message'.format(in_msg, e))
        except JSONDecodeError:
            self.logger.warning('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(in_msg))
        except ValidationError:
            self.logger.warning('Message "{}" failed JSON schema validation (used schema: {})\nIgnoring message'.format(in_msg, os.path.join(self.schema_path,'mushroom_tracking_job.schema.json')))
        else:
            for k,v in self.tracking_results.items():
                response = {}
                response['mushroomTrackingJob'] = msg_json
                response['timestamp'] = time.time()
                response['elapsedTime'] = self.elapsed_time
                response['trackingResult'] = {k:v}
                
                try:
                    resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'mushroom_tracking_output.schema.json', referrer=None)
                    validate(instance=response, schema=self.mushroom_tracking_output_schema, resolver=resolver)
                except ValidationError:
                    self.logger.warning('Response "{}" failed JSON schema validation (used schema: {})\nIgnoring response'.format(response, os.path.join(self.schema_path,'mushroom_tracking_output.schema.json')))
                else:
                    try:
                        self.producer.send(self.out_topic_name, response)
                        self.logger.info('Sending output: {}'.format(response))
                    except KafkaTimeoutError as e:
                        self.logger.error('Timeout error during producer.send(), Potential causes: unable to fetch topic metadata, or unable to obtain memory buffer prior to configured max_block_ms')
                        self.logger.error(e)
            return None

def main():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    args = parser.parse_args()
    mushroom_tracker = KafkaTrack(
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
    mushroom_tracker.start_listening()

if __name__=='__main__':
    main()
