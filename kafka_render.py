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

parser = argparse.ArgumentParser(description="Kafka Renderer")
parser.add_argument("in_topic_name", type=str,help="in_topic name")
parser.add_argument("out_topic_name", type=str,help="out_topic name")
parser.add_argument("bootstrap_servers", type=str, nargs='+', help="list of bootstrap servers")
parser.add_argument("-g", "--group_id", dest='consumer_group_id', type=str, help="consumer group id", default=None)
parser.add_argument("-b","--blender",dest='blender', type=str, help="path to blender executable", default='blender')
parser.add_argument("-e","--error_topic_name",dest='error_topic_name', type=str, help="name of the Kafka topic to log errors to", default='error.log')
parser.add_argument("-d","--debug",dest='debug', action='store_true', help="run the script in debug mode", default=False)


class KafkaRender(KafkaRunner):
    '''Kafka Runner for rendering Blender scenes

    Incoming Kafka messages are validated against the render_job.schema.json and outgoing messages are validated against the render_output.schema.json

    Args:
     - in_topic_name (str): Name of Kafka topic from which incoming messages will be read
     - out_topic_name (str): Name of Kafka topic where outgoing messages will be sent to
     - bootstrap_servers (list of stings): Address of Kafka bootstrap servers

    Keyword args:
     - blender (str): Path to the blender executable (default is 'blender')
     - schema_path (str): Path to the folder containing the json schemas (default is '../json_schemas')
     - consumer_group_id (str): ID of the Kafka consumer group for the consumer of the Kafka Runner (default is None meaning no consumer group is used)
     - error_topic_name (str): Name of the Kafka topic to send error logs to (default is 'error.log')
     - loglevel (logging.DEBUG/WARN/...): Logging level (default is logging.WARN meaning warnings and higher level logs will be reported)
    '''
    def __init__(self,in_topic_name, out_topic_name, bootstrap_servers, blender = 'blender', schema_path='../json_schemas', consumer_group_id = None, error_topic_name='error.log', loglevel = logging.WARN):
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

        # Set local Blender exe path
        self.blender = blender
        self.temp_render_output = os.path.join(os.path.dirname(os.path.abspath(__file__)),'tmp_render_out')
        if not os.path.exists(self.temp_render_output):
            os.mkdir(self.temp_render_output)

        # Load JSON schemas
        self.schema_path = schema_path
        try:
            schema_path = os.path.join(self.schema_path,'render_job.schema.json')
            with open(schema_path, 'r') as f:
                self.render_job_schema = json.loads(f.read())
        except FileNotFoundError:
            self.logger.warn('JSON schema for Kafka message could not be found at path: "{}"\nIncoming messages will NOT be validated!'.format(schema_path))
            self.render_job_schema = {}
        except JSONDecodeError:
            self.logger.warn('JSON schema for Kafka message at path: "{}" could not be decoded (invalid JSON)\nIncoming messages will NOT be validated!'.format(schema_path))
            self.render_job_schema = {}

        try:
            schema_path = os.path.join(self.schema_path,'render_output.schema.json')
            with open(schema_path, 'r') as f:
                self.render_output_schema = json.loads(f.read())
        except FileNotFoundError:
            self.logger.warn('JSON schema for Kafka message could not be found at path: "{}"\nOutgoing messages will NOT be validated!'.format(schema_path))
            self.render_output_schema = {}
        except JSONDecodeError:
            self.logger.warn('JSON schema for Kafka message at path: "{}" could not be decoded (invalid JSON)\nOutgoing messages will NOT be validated!'.format(schema_path))
            self.render_output_schema = {}

    def msg_to_command(self, msg):
        '''Convert incoming Kafka messages to a command for doing the rendering process with Blender
        '''
        # Validate incoming message (is it valid JSON? Is it valid, according to the schema?)
        try:
            msg_json = msg.value
            if isinstance(msg_json, str):
                msg_json = json.loads(msg_json)
            resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'render_job.schema.json', referrer=None)
            validate(instance=msg_json, schema=self.render_job_schema, resolver=resolver)
        except JSONDecodeError:
            self.logger.warn('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(msg.value))
        except TypeError as e:
            self.logger.warn('Message "{}" could not be decoded (invalid input type for JSON decoding). {}\nIgnoring message'.format(msg.value, e))
        except ValidationError:
            self.logger.warn('Message "{}" failed JSON schema validation (used schema: {})\nIgnoring message'.format(msg.value, os.path.join(self.schema_path,'render_job.schema.json')))
        else:
            # If the message is valid:
            frame_start = 0
            frame_num = 1
            use_gpu = False

            # Get Blender scene from Minio storage
            try:
                scene_path = os.path.normpath(msg_json['scenePath']).split(os.path.sep)
                scene_bucket = scene_path[0]
                scene_name = scene_path[-1]
                scene_path = os.path.sep.join(scene_path[1:])
                if not os.path.exists('scenes'):
                    os.mkdir('scenes')
                if not scene_name in os.listdir('scenes'):
                    self.client.download_file(scene_bucket,scene_path,os.path.join('scenes',scene_name))
                scene = os.path.join('scenes',scene_name)
            except RuntimeError:
                self.logger.warning('Could not locate and/or download scene from "{}" bucket and location "{}". Rendering will NOT start!'.format(scene_bucket,scene_path))
            else:
                # Construct and return command if scene is ready
                if 'startFrame' in msg_json:
                    frame_start = msg_json['startFrame']
                if 'numFrames' in msg_json:
                    frame_num = msg_json['numFrames']
                if 'useGPU' in msg_json:
                    use_gpu = msg_json['useGPU']
                if use_gpu:
                    cmd = [self.blender, scene, '--background', '--python', 'render_frames.py', '--', '-fs {}'.format(frame_start), '-fn {}'.format(frame_num), '-o {}'.format(self.temp_render_output), '--gpu']
                else:
                    cmd = [self.blender, scene, '--background', '--python', 'render_frames.py', '--', '-fs {}'.format(frame_start), '-fn {}'.format(frame_num), '-o {}'.format(self.temp_render_output)]
                self.logger.info('Command to be executed: {}'.format(cmd))
                return cmd

    def make_response(self, in_msg, elapsed_time):
        '''Construct a response after the rendering is completed
        '''
        try:
            msg_json = in_msg
            if isinstance(msg_json, str):
                msg_json = json.loads(msg_json)
            resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'render_job.schema.json', referrer=None)
            validate(instance=msg_json, schema=self.render_job_schema, resolver=resolver)
        except TypeError as e:
            self.logger.warn('Message "{}" could not be decoded (invalid input type for JSON decoding). {}\nIgnoring message'.format(in_msg, e))
        except JSONDecodeError:
            self.logger.warn('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(in_msg))
        except ValidationError:
            self.logger.warn('Message "{}" failed JSON schema validation (used schema: {})\nIgnoring message'.format(in_msg, os.path.join(self.schema_path,'render_job.schema.json')))
        else:
            response = {}
            response['renderJob'] = msg_json
            response['timestamp'] = time.time()
            response['elapsedTime'] = elapsed_time
            
            if 'outputLocation' in msg_json:
                output_location = os.path.normpath(msg_json['outputLocation']).split(os.path.sep)
                output_bucket = output_location[0]
                output_minio_path = os.path.sep.join(output_location[1:])
                try:
                    self.client.create_bucket(output_bucket)
                    self.client.upload_directory(output_bucket, output_minio_path, self.temp_render_output)
                except RuntimeError:
                    self.logger.error('Could not upload rendered frames to Minio storage! (target bucket:{}, target Minio path: {}, local path:{})'.format(output_bucket,output_minio_path,self.temp_render_output))
                else:
                    self.logger.info('Removing temporary local copy of rendered frames')
                    for fname in os.listdir(self.temp_render_output):
                        os.remove(os.path.join(self.temp_render_output, fname))
            else:
                self.logger.warn('No output location specified in incoming message! Rendered frames will only be saved locally!')
            try:
                resolver = RefResolver(base_uri='file://'+os.path.abspath(self.schema_path)+'/'+'render_output.schema.json', referrer=None)
                validate(instance=response, schema=self.render_output_schema, resolver=resolver)
            except ValidationError:
                self.logger.warn('Response "{}" failed JSON schema validation (used schema: {})\nIgnoring response'.format(response, os.path.join(self.schema_path,'render_output.schema.json')))
            else:
                return response

def main():
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    args = parser.parse_args()
    renderer = KafkaRender(args.in_topic_name, args.out_topic_name, args.bootstrap_servers, args.blender, consumer_group_id=args.consumer_group_id, error_topic_name=args.error_topic_name, loglevel=logging.DEBUG if args.debug else logging.WARN)
    renderer.start_listening()

if __name__=='__main__':
    main()