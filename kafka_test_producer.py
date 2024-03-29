import json
import time
import argparse
from kafka import KafkaProducer

parser = argparse.ArgumentParser(description="Publish messages in Kafka topic")
parser.add_argument("topic_name", type=str,help="topic name")
parser.add_argument("bootstrap_servers", type=str, nargs='+', help="list of bootstrap servers")
parser.add_argument("-m","--message", dest='message',type=str, help="message to publish", default='HELLO!')
parser.add_argument("-i","--intrval", dest='interval',type=int, help="message repetition interval in seconds", default=2)
parser.add_argument("-r","--repeat", dest='repeat', action='store_true', help="repeat")

class KafkaTestProducer():
    '''Simple cmd line producer client for Kafka

    Args:
     - topic_name (str): Topic to publish to
     - bootstrap_servers (list of strings): Address of Kafka bootstrap servers
     - message (str): Message to send (default is "HELLO!")
     - interval (int): How many seconds to wait between sending messages (only works if repeat is True) (default is 2)
     - repeat (bool): Repeat sending messages (default is False)
    '''
    def __init__(self, topic_name: str, bootstrap_servers: list[str], message: str='HELLO!', interval: int=2, repeat: bool=False):
        self.topic_name = topic_name
        self.bootstrap_servers = bootstrap_servers
        self.message = message
        self.interval = interval
        self.repeat = repeat
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=10000,
            reconnect_backoff_max_ms=100
            )

    def send_message(self) -> None:
        '''Send message to Kafka topic
        '''
        if self.repeat:
            while True:
                try:
                    self.producer.send(self.topic_name, self.message)
                    print('Sent message: {}'.format(self.message))
                    time.sleep(self.interval)
                except KeyboardInterrupt:
                    break
        else:
            self.producer.send(self.topic_name, self.message)
            print('Sent message: {}'.format(self.message))

def main() -> None:
    '''Main function
    '''
    args = parser.parse_args()
    kafka_test_producer = KafkaTestProducer(args.topic_name, args.bootstrap_servers, args.message, args.interval, args.repeat)
    kafka_test_producer.send_message()

if __name__=='__main__':
    main()