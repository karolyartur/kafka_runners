# Kafka Runners

This repository hosts the specialized Kafka Runner software modules.

## How to use

### `KafkaRunner` base class

A Kafka Runner listens to incoming messages in a Kafka topic, from the incoming messages constructs commands to be run in the command line, executes these commands and after the execution is complete it sends a response in an other Kafka topic.

The script [kafka_runner.py](kafka_runner.py) defines the `KafkaRunner` class, which is the base class for all Kafka Runners.

The `KafkaRunner` class provides the basic functionality:
 - a `consumer` to listen to incoming messages in a specified Kafka topic
 - a `producer` to send outgoing messages to a specified Kafka topic
 - a `logger` to log to the console and also to a Kafka topic

The `start_listening()` member function of the `KafkaRunner` class should be called to start listening to incoming messages. This is a blocking function that runs until `KeyboardInterrupt` is raised.

In Kafka Runners the `msg_to_command(self, msg)` member function should be overwritten to give the Kafka Runner its specific functionality. This member function is called whenever the consumer gets a new Kafka message. **The function should return a list containing a command that can be passed to `subprocess.Popen()` for execution.** For example: `['echo', 'Hello World!']`.

The function `make_response(self, in_msg, elapsed_time)` should be overwritten if the Kafka Runner needs to send messages after the execution of the command is completed. **The function should return a Python dictionary.**

### Minio operations

The script [minio_client.py](minio_client.py) defines the `MinioClient` class that can be used to upload/download object to/from the Minio object storage.


### Testing

The scripts [kafka_test_producer.py](kafka_test_producer.py) and [kafka_topic_echo.py](kafka_topic_echo.py) can be used to quickly check functionality and debug Kafka Runners.