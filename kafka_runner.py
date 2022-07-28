import json
import time
import argparse
import subprocess
from kafka import KafkaConsumer

parser = argparse.ArgumentParser(description="Kafka Runner")
parser.add_argument("topic_name", type=str,help="topic name")
parser.add_argument("bootstrap_servers", type=str, nargs='+', help="list of bootstrap servers")


class KafkaRunner():
    '''
        Simple consumer for running command based on messages from Kafka
    '''
    def __init__(self, topic_name, bootstrap_servers):
        self.timeout = 30
        self.topic_name = topic_name
        self.bootstrap_servers = bootstrap_servers
        self.consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.bootstrap_servers,
            request_timeout_ms=5000,
            reconnect_backoff_max_ms=50,
            session_timeout_ms=self.timeout*1000,
            consumer_timeout_ms=self.timeout*1000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
    
    def start_listening(self):
        while True:
            try:
                for msg in self.consumer:
                    process = subprocess.Popen(self.msg_to_command(msg),
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE,
                        universal_newlines=True)
                    stdout, stderr = process.communicate()
                    print(stdout.strip())
                print('Idle ...')
                time.sleep(self.timeout)
            except KeyboardInterrupt:
                break

    def msg_to_command(self, msg):
        return(['echo','{}'.format(msg.value)])


def main():
    args = parser.parse_args()
    kafka_runner = KafkaRunner(args.topic_name, args.bootstrap_servers)
    kafka_runner.start_listening()

if __name__=='__main__':
    main()