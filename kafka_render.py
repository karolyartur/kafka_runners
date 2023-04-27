import argparse
from kafka_runner import KafkaRunner

parser = argparse.ArgumentParser(description="Kafka Runner")
parser.add_argument("topic_name", type=str,help="topic name")
parser.add_argument("bootstrap_servers", type=str, nargs='+', help="list of bootstrap servers")


class KafkaRender(KafkaRunner):
    def msg_to_command(self, msg):
        value = msg.value
        frame_start = value['frame_start']
        frame_num = value['frame_num']
        use_gpu = value['use_gpu']
        if use_gpu:
            cmd = ['blender', 'mushroom_2.blend', '--background', '--python', 'render_frames.py', '--', '-fs {}'.format(frame_start), '-fn {}'.format(frame_num), '--gpu']
        else:
            cmd = ['blender', 'mushroom_2.blend', '--background', '--python', 'render_frames.py', '--', '-fs {}'.format(frame_start), '-fn {}'.format(frame_num)]

        return cmd

def main():
    args = parser.parse_args()
    renderer = KafkaRender(args.topic_name, args.bootstrap_servers)
    renderer.start_listening()

if __name__=='__main__':
    main()