import os
import io
import socket
import json
import time
import subprocess
import logging
import traceback
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaConfigurationError, KafkaTimeoutError
from jsonschema import validate, RefResolver
from jsonschema.exceptions import ValidationError
from json.decoder import JSONDecodeError

from collections.abc import Callable
from typing import Any
from abc import ABC, abstractmethod
from dataclasses import dataclass, KW_ONLY

@dataclass
class KafkaRunner(ABC):
    '''Abstract class to serve as base class for all Kafka Runners

    This class cannot be used directly, a child class has to be made that overrides the abstract methods.
    
    Usage:
        class MyNewKafkaRunner(KafkaRunner):
            ...
            def proc_incoming_msg(self, msg):
                ...
            def make_response(self, in_msg, elapsed_time):
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
     - service_info_topic_name (str): Name of Kafka topic where service info will be sent to (default is 'service.info')
     - loglevel (logging.DEBUG/WARN/...): Logging level (default is logging.WARN meaning warnings and higher level logs will be reported)
     - timeout (int): Number of seconds before Kafka consumer timeouts (one Idle cycle) (default is 30)
     - self_kill (bool): Flag indicating whether to shut down automatically to clear memory (default is True)
     - kill_idle_count (int): Number of Idle cycles to do after a successful task before shutting down. Only works if self_kill is True (default is 5)
    '''
    in_topic_name: str
    out_topic_name: str
    bootstrap_servers: list[str]
    _: KW_ONLY
    consumer_group_id: str | None = None
    error_topic_name: str = 'error.log'
    service_info_topic_name: str = 'service.info'
    loglevel: int = logging.WARN
    timeout: int = 30
    self_kill: bool = True
    kill_idle_count: int = 5


    def __post_init__(self):
        '''This will be called at the end of the __init__ method
        '''
        # Init optional attributes
        self.service_info = None  # Can be used to store service info message (if set, service info will be published through Kafka)

        # Setup logging
        self._setup_logger()

        # Setup Kafka consumer and producer
        self.consumer = self._setup_kafka_consumer()
        self.producer = self._setup_kafka_producer()

        # Decorate logger's error function, so it also sends errors in Kafka 
        self.logger.error = self._error_log_decorator(self.logger.error)


    @abstractmethod
    def proc_incoming_msg(self, msg_json: dict[str,Any]) -> list[str] | None:
        '''Process incoming messages

        This function should be overwritten in child classes.

        Args:
         - msg_json (dict with str keys and any type of value): Incoming message

        Returns:
         - cmd (list of strings): Command to be executed by subprocess.Popen() (for example: ['echo', 'Hello World!']) (if None, no additional command will be executed)
        '''
        pass


    @abstractmethod
    def make_response(self, msg_json: dict[str,Any], elapsed_time: float) -> dict[str,Any] | None:
        '''Construct a response

        This function should be overwritten in child classes.

        Args:
         - msg_json (dict with str keys and any type of value): Incoming message
         - elapsed_time (float): Time elapsed during performing the task

        Returns:
         - response (dict with str keys and any type of value): Response to be sent (if None, no response will be sent)
        '''
        pass


    def start_listening(self) -> None:
        '''Start listening to incoming messages

        This function should be called to use the Kafka Runner. It blocks until a KeyboardInterrupt is encountered
        or after "kill_idle_count" number of Idle cycles after completing a task if "self_kill" is Ture 
        '''
        performed_task = False  # Stores if the service already performed a task or not
        idle_count = 0  # Stores the number of idle cycles
        while True:
            try:
                for msg in self.consumer:
                    # Loop through messages from the Kafka topic
                    self.logger.info('Got a new message: {}'.format(msg.value))

                    # Convert the message to a dict expecting JSON format
                    try:
                        msg_json = msg.value
                        if isinstance(msg_json, str):
                            msg_json = json.loads(msg_json)
                    except JSONDecodeError:
                        self.logger.warning('Message "{}" could not be decoded (invalid JSON)\nIgnoring message'.format(msg.value))
                    except TypeError as e:
                        self.logger.warning('Message "{}" could not be decoded (invalid input type for JSON decoding). {}\nIgnoring message'.format(msg.value, e))
                    else:
                        # Message successfully converted to dict
                        start_time = time.time()
                        cmd = self.proc_incoming_msg(msg_json)  # Call the implementation of the abstract function
                        if cmd:
                            # Execute command if a command is returned
                            self.logger.info('Executing command')
                            try:
                                process = subprocess.Popen(cmd,
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
                                stdout, stderr = process.communicate()
                                self.logger.info(stdout.strip())
                                if stderr.strip():
                                    self.logger.error(stderr.strip())
                            except FileNotFoundError as e:
                                self.logger.error('The construced command could not be executed!')
                                self.logger.error(e)
                            else:
                                # The command finished cleanly
                                self.logger.info('Finished executing command.')
                        elapsed_time = time.time()-start_time
                        self.logger.info('Elapsed time: {}'.format(elapsed_time))
                        # Construct response
                        response = self.make_response(msg_json, elapsed_time)  # Call the implementation of the abstract function
                        if response:
                            # Send a response if there is any
                            try:
                                self.producer.send(self.out_topic_name, response)
                                self.logger.info('Sending output: {}'.format(response))
                            except KafkaTimeoutError as e:
                                self.logger.error('Timeout error during producer.send(), Potential causes: unable to fetch topic metadata, or unable to obtain memory buffer prior to configured max_block_ms')
                                self.logger.error(e)
                        performed_task = True

                # If the Kafka consumer reaches timeout it is considered an Idle cycle
                print('Idle ...')

                # Send service info
                if self.service_info:
                    try:
                        self.producer.send(self.service_info_topic_name, self.service_info)
                    except KafkaTimeoutError as e:
                        self.logger.error('Timeout error during producer.send(), Potential causes: unable to fetch topic metadata, or unable to obtain memory buffer prior to configured max_block_ms')
                        self.logger.error(e)

                # Check if the Idle is reached after performing a task and shutdown if needed
                if performed_task:
                    idle_count += 1
                if self.self_kill and idle_count > self.kill_idle_count:
                    break
            
            except KeyboardInterrupt:
                break


    def _setup_logger(self) -> None:
        '''Create and assign a logger to the object

        Also adds a string stream where error logs are pushed into for sending them through Kafka
        '''
        # Create logger
        self.logger = logging.getLogger(__name__ + '.' + type(self).__name__ + '@' + socket.gethostname())
        self.logger.setLevel(self.loglevel)
        self.error_stream = io.StringIO()
        
        # String stream logger for error logging
        sh = logging.StreamHandler(self.error_stream)
        sh.setLevel(logging.ERROR)
        sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(sh)


    def _setup_kafka_consumer(self) -> KafkaConsumer:
        '''Create a Kafka consumer

        Returns:
         - consumer (KafkaConsumer): The created Kafka consumer
        '''
        try:
            consumer = KafkaConsumer(
                self.in_topic_name,
                bootstrap_servers=self.bootstrap_servers,
                group_id = self.consumer_group_id,
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
        else:
            self.logger.info('Kafka consumer created!')
            return consumer


    def _setup_kafka_producer(self) -> KafkaProducer:
        '''Create a Kafka producer

        Returns:
         - producer (KafkaProducer): The created Kafka producer
        '''
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                request_timeout_ms=10000,
                reconnect_backoff_max_ms=100
                )
        except KafkaConfigurationError as e:
            self.logger.error('Could not create the Kafka producer!')
            self.logger.error(traceback.format_exc())
            raise RuntimeError('Could not create the Kafka producer!') from e
        else:
            self.logger.info('Kafka producer created!')
            return producer


    def _error_log_decorator(self, func: Callable) -> Callable:
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


    def construct_and_validate_service_info(
            self,
            schema_path: str,
            service_name: str,
            service_description: str='',
            input_topic_schema: dict[str,Any]={},
            output_topic_schema: dict[str,Any]={}) -> dict[str,Any]:
        '''Helper function for constructing and validating service info JSON messages

        To ensure the service advertises service info use it like this:
        self.service_info = self.construct_and_validate_service_info(...)

        Args:
         - schema_path (str): Path to the folder containing the JSON schemas
         - service_name (str): Name of the service
         - service_description (str): Short description of the service's functionality and purpose
         - input_topic_schema (dict with str keys and any type of value): JSON schema for input messages
         - output_topic_schema (dict with str keys and any type of value): JSON schema for output messages

        Returns:
         - service_info (dict with str keys and any type of value): Service info message
        '''
        self.service_info_schema = {}
        # Load Service Info JSON schema
        try:
            schema_path = os.path.join(schema_path,'service_info.schema.json')
            with open(schema_path, 'r') as f:
                self.service_info_schema = json.loads(f.read())
        except FileNotFoundError:
            self.logger.warning('JSON schema for Kafka message could not be found at path: "{}"\nService Info messages will NOT be validated!'.format(schema_path))
            self.service_info_schema = {}
        except JSONDecodeError:
            self.logger.warning('JSON schema for Kafka message at path: "{}" could not be decoded (invalid JSON)\nService Info messages will NOT be validated!'.format(schema_path))
            self.service_info_schema = {}

        # Construct service info
        service_info = {}
        service_info['serviceName'] = service_name
        service_info['serviceDescription'] = service_description
        service_info['inputTopics']= [{'topicName': self.in_topic_name, 'schema': input_topic_schema}]
        service_info['outputTopics'] = [{'topicName': self.out_topic_name, 'schema': output_topic_schema}, {'topicName': self.error_topic_name, 'schema': {}}, {'topicName': self.service_info_topic_name, 'schema': self.service_info_schema}]

        try:
            resolver = RefResolver(base_uri='file://'+os.path.abspath(schema_path)+'/'+'service_info.schema.json', referrer=None)
            validate(instance=service_info, schema=self.service_info_schema, resolver=resolver)
        except ValidationError:
            self.logger.warning('Service Info "{}" failed JSON schema validation (used schema: {})\nIgnoring message'.format(service_info, os.path.join(schema_path,'service_info.schema.json')))
        else:
            return service_info