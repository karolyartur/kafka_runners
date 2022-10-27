# Kafka Mask-RCNN Trainer

This is a Kafka Runner, that can be used for Training Mask-RCNN.

## How to use

The trainer waits for messages from a Kafka topic. When a message is received and it passes validation, the training of the network is performed according to the contents of the incoming message. After the training is completed, an output message will be sent in a Kafka topic.

The incoming and outgoing messages are validated against the json schemas:
 - **training_job.schema.json**
 - **training_output.schema.json**

See the schema descriptions in the [repository for json schemas](http://10.8.8.219/fungi/json_schemas).

To start the training service, start the [kafka_train.py](kafka_train.py) script from the command line like so:
```bash
python kafka_train.py IN.TOPIC.NAME OUT.TOPIC.NAME BOOTSTRAP.SERVER.ADDRESS
```
Where `IN.TOPIC.NAME` should be the name of the Kafka topic for incoming messages (training jobs), `OUT.TOPIC.NAME` should be the name of the Kafka topic for outgoing messages (training outputs), and BOOTSTRAP.SERVER.ADDRESS is the address of the Kafka bootstrap server. In the case when there are multiple bootstrap servers, they should be listed with spaces in between.

An example command-line use, with an input topic name of `train.job`, an output topic name of `train.out` and bootstrap servers 10.8.8.200:9090 and 10.8.8.201:9094 would be
```bash
python kafka_train.py train.job train.out 10.8.8.200:9090 10.8.8.201:9094
```

Apart from these compulsory positional arguments the script can also be used with keyword arguments. These are the following:
 - `-g` or `--group_id` can be used to specify the consumer group ID for the Kafka consumer (by default this is not used)
 - `-e` or `--error_topic_name` can be used to specify the Kafka topic in which the error messages will be sent (by default errors will be sent to the `error.log` Kafka topic)
 - `-d` or `--debug` is a flag that can be set to run the script in debug mode

Using the previous example, but with specifying the consumer group ID as `my_consumer_group`, the error topic name as `train.errors` and running the script in debug mode would look like this:
```bash
python kafka_train.py train.job train.out 10.8.8.200:9090 10.8.8.201:9094 -g my_consumer_group -e train.errors -d
```

When the script is up and running you can start sending messages to the specified Kafka topic and wait wait for responses in the output topic. The output of the training should be uploaded to the Minio object storage at the location specified in the training job.

## Technical details and deployment

The trainer creates its own Minio client, which looks for the credentials in `../credentials`. Before running the renderer you have to make sure, that the credentials file (minio_credentials.json) is available at that location.

The json schemas are searched for at the `../json_schemas` location. Before running the renderer you have to make sure, that the schemas are available at that location. If not, the script will still run, but incoming and outgoing messages will not be validated.

When an incoming Kafka message is received, the requested config file and class info file are downloaded from the Minio storage (to the location `config`). This **ONLY** happens if there is not a local copy of these files already!
When these files and the data are available, the MRCNNTrainer class of the [train_mrcnn.py](train_mrcnn.py) script is used to train the network. This uses internally the Mask-RCNN implementation from [here](https://github.com/matterport/Mask_RCNN). This repository must be available next to the kafka_runners repository!

The output of the training is saved at the `training_output` folder locally. When the whole training process is finished, the contents of this folder are uploaded to the requested location in the Minio storage, and the local `training_output` folder is emptied. The output from the trainer containing the time taken for the training process, as well as other information are also sent to the output topic.

# Kafka Runners

This repository hosts the specialized Kafka Runner software modules.

## How to use

### `KafkaRunner` base class

A Kafka Runner listens to incoming messages in a Kafka topic, from the incoming messages constructs commands to be run in the command line, executes these commands and after the execution is complete it sends a response in an other Kafka topic.

The script [kafka_runner.py](kafka_runner.py) defines the `KafkaRunner` class, which is the base class for all Kafka Runners. When creating your own Kafka Runner make it derive from this class. **All Kafka Runners should have their dedicated branches in the repository!**

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
