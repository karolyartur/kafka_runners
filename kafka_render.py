import argparse
import json
from kafka_runner import KafkaRunner

parser = argparse.ArgumentParser(description="Kafka Runner")
parser.add_argument("topic_name", type=str,help="topic name")
parser.add_argument("bootstrap_servers", type=str, nargs='+', help="list of bootstrap servers")
parser.add_argument("-b","--blender",dest='blender', type=str, help="path to blender executable", default='blender')
parser.add_argument("-s","--scene",dest='scene', type=str, help="path to blend file containing the scene", default='mushroom_2.blend')


class KafkaRender(KafkaRunner):
    def __init__(self,topic_name, bootstrap_servers, blender = 'blender', scene='scene'):
        self.blender = blender
        self.scene = scene
        super().__init__(topic_name, bootstrap_servers)

    def msg_to_command(self, msg):
        value = json.loads(msg.value)
        print(value)
        print(type(value))
        frame_start = value['frame_start']
        frame_num = value['frame_num']
        use_gpu = value['use_gpu']
        if use_gpu:
            cmd = [self.blender, self.scene, '--background', '--python', 'render_frames.py', '--', '-fs {}'.format(frame_start), '-fn {}'.format(frame_num), '--gpu']
        else:
            cmd = [self.blender, self.scene, '--background', '--python', 'render_frames.py', '--', '-fs {}'.format(frame_start), '-fn {}'.format(frame_num)]
        print(cmd)
        return cmd

def main():
    args = parser.parse_args()
    renderer = KafkaRender(args.topic_name, args.bootstrap_servers, args.blender, args.scene)
    renderer.start_listening()

if __name__=='__main__':
    main()