import io
import socket
import json
import time
import argparse
import subprocess
import logging
import traceback
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaConfigurationError, KafkaTimeoutError

parser = argparse.ArgumentParser(description="Kafka Runner")
parser.add_argument("in_topic_name", type=str,help="in_topic name")
parser.add_argument("out_topic_name", type=str,help="out_topic name")
parser.add_argument("bootstrap_servers", type=str, nargs='+', help="list of bootstrap servers")


class KafkaRunner():
    '''Base class for all Kafka Runners

    Can be used directly but does not provide useful functionality (incoming messages are echoed in the console and no outgoing messages).
    
    Usage:
        class MyNewKafkaRunner(KafkaRunner):
            ...
            def self.msg_to_command(self, msg):
                ...
            def self.make_response(self, in_msg, elapsed_time):
                ...
        
        runner = MyNewKafkaRunner(...)
        runner.start_listening()

    Args:
     - in_topic_name (str): Name of Kafka topic from which incoming messages will be read
     - out_topic_name (str): Name of Kafka topic where outgoing messages will be sent to
     - bootstrap_servers (list of stings): Address of Kafka bootstrap servers

    Keyword args:
     - consumer_group_id (str): ID of the Kafka consumer group for the consumer of the Kafka Runner (default is None meaning no consumer group is used)
     - error_topic_name (str): Name of the Kafka topic to send error logs to (default is 'error.log')
     - loglevel (logging.DEBUG/WARN/...): Logging level (default is logging.WARN meaning warnings and higher level logs will be reported)
    '''
    def __init__(self, in_topic_name, out_topic_name, bootstrap_servers, consumer_group_id=None, error_topic_name='error.log', loglevel=logging.WARN):
        '''Constructor for Kafka Runners
        '''
        self.timeout = 30
        self.in_topic_name = in_topic_name
        self.out_topic_name = out_topic_name
        self.bootstrap_servers = bootstrap_servers
        self.consumer_group_id = consumer_group_id
        self.error_topic_name = error_topic_name

        # Create logger
        self.logger = logging.getLogger(__name__ + '.' + type(self).__name__ + '@' + socket.gethostname())
        self.logger.setLevel(loglevel)
        self.error_stream = io.StringIO()
        
        # String stream logger for error logging
        sh = logging.StreamHandler(self.error_stream)
        sh.setLevel(logging.ERROR)
        sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(sh)

        try:
            self.consumer = KafkaConsumer(
                self.in_topic_name,
                bootstrap_servers=self.bootstrap_servers,
                group_id = consumer_group_id,
                request_timeout_ms=50000,
                reconnect_backoff_max_ms=50,
                session_timeout_ms=self.timeout*1000,
                consumer_timeout_ms=self.timeout*1000,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
        except KafkaConfigurationError as e:
            self.logger.error('Could not create the Kafka consumer!')
            self.logger.error(traceback.format_exc())
            raise RuntimeError('Could not create the Kafka consumer!') from e

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=10000,
                reconnect_backoff_max_ms=100
                )
        except KafkaConfigurationError as e:
            self.logger.error('Could not create the Kafka producer!')
            self.logger.error(traceback.format_exc())
            raise RuntimeError('Could not create the Kafka producer!') from e

        self.logger.error = self._error_log_decorator(self.logger.error)
    
    def start_listening(self):
        '''Start listening to incoming messages

        This function should be called to use the Kafka Runner. It blocks until a KeyboardInterrupt is encountered
        '''
        while True:
            try:
                for msg in self.consumer:
                    self.logger.info('Got a new message: {}'.format(msg.value))
                    cmd = self.msg_to_command(msg)
                    if cmd:
                        self.logger.info('Executing command')
                        start_time = time.time()
                        process = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE, 
                            stderr=subprocess.PIPE,
                            universal_newlines=True)
                        stdout, stderr = process.communicate()
                        end_time = time.time()
                        self.logger.info('Finished executing command. Elapsed time: {}'.format(end_time-start_time))
                        response = self.make_response(msg.value, end_time-start_time)
                        if response:
                            self.producer.send(self.out_topic_name, response)
                            self.logger.info('Sending output: {}'.format(response))
                        print(stdout.strip())
                print('Idle ...')
                time.sleep(self.timeout)
            except FileNotFoundError as e:
                self.logger.error('The construced command could not be executed!')
                self.logger.error(e)
            except KafkaTimeoutError as e:
                self.logger.error('Timeout error during producer.send(), Potential causes: unable to fetch topic metadata, or unable to obtain memory buffer prior to configured max_block_ms')
                self.logger.error(e)
            except KeyboardInterrupt:
                break

    def msg_to_command(self, msg):
        '''Convert incoming message to a command

        This function should be overwritten in derived classes

        Args:
         - msg (Kafka message object): Incoming message (the contents can be accessed by msg.value, which is a string)

        Returns:
         - cmd (list of strings): Command to be executed by subprocess.Popen() (for example: ['echo', 'Hello World!'])
        '''
        return(['echo','{}'.format(msg.value)])

    def make_response(self, in_msg, elapsed_time):
        '''Construct a response

        This function should be overwritten in derived classes

        Args:
         - in_msg (str): Contents of incoming message
         - elapsed_time (float): Time needed for the execution of the command in seconds

        Returns:
         - response (dictionary): Response to be sent (if None, no response will be sent)
        '''
        return None

    def _error_log_decorator(self, func):
        '''Decorator for INTERNAL USE ONLY

        Used for decorating the self.logger.error function to send error logs to a Kafka topic as well
        '''
        def wrapper(*args, **kwargs):
            func(*args,**kwargs)
            try:
                self.producer.send(self.error_topic_name, self.error_stream.getvalue())
                self.logger.info('Sending error report to Kafka')
            except KafkaTimeoutError:
                self.logger.warn('Timeout error during producer.send(), Potential causes: unable to fetch topic metadata, or unable to obtain memory buffer prior to configured max_block_ms! Error will not be sent to Kafka!')
            self.error_stream.seek(0)
            self.error_stream.truncate()
        return wrapper

def main():
    args = parser.parse_args()
    kafka_runner = KafkaRunner(args.in_topic_name, args.out_topic_name, args.bootstrap_servers)
    kafka_runner.start_listening()

if __name__=='__main__':
    main()