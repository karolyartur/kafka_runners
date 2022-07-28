import json
import time
import argparse
from kafka import KafkaConsumer

parser = argparse.ArgumentParser(description="Echo messages in Kafka topic")
parser.add_argument("topic_name", type=str,help="topic name")
parser.add_argument("bootstrap_servers", type=str, nargs='+', help="list of bootstrap servers")

class KafkaTopicEcho():
    '''
        Simple cmd line echo consumer client for Kafka
    '''
    def __init__(self, topic_name, bootstrap_servers):
        self.topic_name = topic_name
        self.bootstrap_servers = bootstrap_servers
        self.consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.bootstrap_servers,
            request_timeout_ms=5000,
            reconnect_backoff_max_ms=50,
            session_timeout_ms=30000,
            consumer_timeout_ms=30000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )


def main():
    args = parser.parse_args()
    kafka_echo = KafkaTopicEcho(args.topic_name, args.bootstrap_servers)
    while True:
        try:
            for msg in kafka_echo.consumer:
                print(msg.value)
            print('Idle ...')
            time.sleep(30)
        except KeyboardInterrupt:
            break

if __name__=='__main__':
    main()