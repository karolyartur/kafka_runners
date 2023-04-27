import os
import argparse
import json
import logging
import time
from kafka_runner import KafkaRunner
from minio_client import MinioClient
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from json.decoder import JSONDecodeError

parser = argparse.ArgumentParser(description="Kafka Renderer")
parser.add_argument("in_topic_name", type=str,help="in_topic name")
parser.add_argument("out_topic_name", type=str,help="out_topic name")
parser.add_argument("bootstrap_servers", type=str, nargs='+', help="list of bootstrap servers")
parser.add_argument("-g", "--group_id", dest='consumer_group_id', type=str, help="consumer group id", default=None)
parser.add_argument("-b","--blender",dest='blender', type=str, help="path to blender executable", default='blender')
parser.add_argument("-d","--debug",dest='debug', action='store_true', help="run the script in debug mode", default=False)


class KafkaRender(KafkaRunner):
    ## SCHEMA PATH FROM COMMAND LINE OR DIRECT ENV VAR? --> schema in git repo -->> docker run env var --> commit hash --> deploy key??
    ## --> error topic in kafka -->> docker env variable (docker container hash) unique ID for container name + timestamp
    def __init__(self,in_topic_name, out_topic_name, bootstrap_servers, blender = 'blender', schema_path='../json_schemas/render_job.schema.json', consumer_group_id = None, loglevel = logging.WARN):
        # Init base-class
        try:
            super().__init__(in_topic_name, out_topic_name, bootstrap_servers, consumer_group_id=consumer_group_id)
        except RuntimeError:
            exit(0)
        self.logger.setLevel(loglevel)

        # Connect to Minio object storage
        try:
            self.client = MinioClient(logger = self.logger)
        except (RuntimeError,ValueError):
            exit(0)

        # Set local Blender exe path
        self.blender = blender

        # Load JSON schemas
        self.schema_path = schema_path
        try:
            with open(self.schema_path, 'r') as f:
                self.schema = json.loads(f.read())
        except FileNotFoundError:
            self.logger.warn('JSON schema for Kafka message could not be found at path: "{}"\nIncoming messages will NOT be validated!'.format(schema_path))
            self.schema = {}
        except JSONDecodeError:
            self.logger.warn('JSON schema for Kafka message at path: "{}" could not be decoded (invalid JSON)\nIncoming messages will NOT be validated!'.format(schema_path))
            self.schema = {}

    def msg_to_command(self, msg):
        # Validate incoming message (is it valid JSON? Is it valid, according to the schema?)
        try:
            msg_json = json.loads(msg.value)
            validate(instance=msg_json, schema=self.schema)
        except JSONDecodeError:
            self.logger.warn('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(msg.value))
        except ValidationError:
            self.logger.warn('Message "{}" failed JSON schema validation (used schema: {})\nIgnoring message'.format(msg.value, self.schema_path))
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
                    cmd = [self.blender, scene, '--background', '--python', 'render_frames.py', '--', '-fs {}'.format(frame_start), '-fn {}'.format(frame_num), '--gpu']
                else:
                    cmd = [self.blender, scene, '--background', '--python', 'render_frames.py', '--', '-fs {}'.format(frame_start), '-fn {}'.format(frame_num)]
                self.logger.info('Command to be executed: {}'.format(cmd))
                #return cmd
                return ['sleep', '5']

    def make_response(self, in_msg, elapsed_time):
        try:
            msg_json = json.loads(in_msg)
            validate(instance=msg_json, schema=self.schema)
        except JSONDecodeError:
            self.logger.warn('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(msg.value))
        except ValidationError:
            self.logger.warn('Message "{}" failed JSON schema validation (used schema: {})\nIgnoring message'.format(msg.value, self.schema_path))
        else:
            response = {}
            response['RenderJob'] = msg_json
            response['timestamp'] = time.time()
            response['elapsedTime'] = elapsed_time
            response['outputLocation'] = '.'
            return response

def main():
    logging.basicConfig()
    args = parser.parse_args()
    renderer = KafkaRender(args.in_topic_name, args.out_topic_name, args.bootstrap_servers, args.blender, consumer_group_id=args.consumer_group_id, loglevel=logging.DEBUG if args.debug else logging.WARN)
    renderer.start_listening()

if __name__=='__main__':
    main()