import argparse
import json
import logging
import time
from kafka_runner import KafkaRunner
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from json.decoder import JSONDecodeError

parser = argparse.ArgumentParser(description="Kafka Runner")
parser.add_argument("in_topic_name", type=str,help="in_topic name")
parser.add_argument("out_topic_name", type=str,help="out_topic name")
parser.add_argument("bootstrap_servers", type=str, nargs='+', help="list of bootstrap servers")
parser.add_argument("-g", "--group_id", dest='consumer_group_id', type=str, help="consumer group id", default=None)
parser.add_argument("-b","--blender",dest='blender', type=str, help="path to blender executable", default='blender')
parser.add_argument("-s","--scene",dest='scene', type=str, help="path to blend file containing the scene", default='mushroom_2.blend')
parser.add_argument("-d","--debug",dest='debug', action='store_true', help="run the script in debug mode", default=False)


class KafkaRender(KafkaRunner):
    ## --> scene az object storage-bol
    ## --> add timings to cmd run --> to output
    ## SCHEMA PATH FROM COMMAND LINE OR DIRECT ENV VAR? --> schema in git repo -->> docker run env var --> commit hash --> deploy key??
    ## CONSUMER GROUP ID CAUSES KAFKA CONFIG ERROR --> request timeout > session timeout
    ## --> invalid schema --> except
    ## --> error topic in kafka -->> docker env variable (docker container hash) unique ID for container name + timestamp
    def __init__(self,in_topic_name, out_topic_name, bootstrap_servers, blender = 'blender', scene='scene', schema_path='../json_schemas/render_job.schema.json', consumer_group_id = None, loglevel = logging.WARN):
        self.blender = blender
        self.scene = scene
        self.schema_path = schema_path
        try:
            super().__init__(in_topic_name, out_topic_name, bootstrap_servers, consumer_group_id=consumer_group_id)
        except RuntimeError as e:
            exit(0)
        try:
            with open(self.schema_path, 'r') as f:
                self.schema = json.loads(f.read())
        except FileNotFoundError:
            self.logger.warn('JSON schema for Kafka message could not be found at path: "{}"\nIncoming messages will NOT be validated!'.format(schema_path))
            self.schema = {}
        except JSONDecodeError:
            self.logger.warn('JSON schema for Kafka message at path: "{}" could not be decoded (invalid JSON)\nIncoming messages will NOT be validated!'.format(schema_path))
            self.schema = {}
        self.logger.setLevel(loglevel)

    def msg_to_command(self, msg):
        try:
            msg_json = json.loads(msg.value)
            validate(instance=msg_json, schema=self.schema)
        except JSONDecodeError:
            self.logger.warn('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(msg.value))
        except ValidationError:
            self.logger.warn('Message "{}" failed JSON schema validation (used schema: {})\nIgnoring message'.format(msg.value, self.schema_path))
        else:
            frame_start = 0
            frame_num = 1
            use_gpu = False
            if 'startFrame' in msg_json:
                frame_start = msg_json['startFrame']
            if 'numFrames' in msg_json:
                frame_num = msg_json['numFrames']
            if 'useGPU' in msg_json:
                use_gpu = msg_json['useGPU']
            if use_gpu:
                cmd = [self.blender, self.scene, '--background', '--python', 'render_frames.py', '--', '-fs {}'.format(frame_start), '-fn {}'.format(frame_num), '--gpu']
            else:
                cmd = [self.blender, self.scene, '--background', '--python', 'render_frames.py', '--', '-fs {}'.format(frame_start), '-fn {}'.format(frame_num)]
            self.logger.info('Command to be executed: {}'.format(cmd))
            return cmd
            # return ['sleep', '5']

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
    renderer = KafkaRender(args.in_topic_name, args.out_topic_name, args.bootstrap_servers, args.blender, args.scene, consumer_group_id=args.consumer_group_id, loglevel=logging.DEBUG if args.debug else logging.WARN)
    renderer.start_listening()

if __name__=='__main__':
    main()