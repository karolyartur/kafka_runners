# Kafka Render

This is a Kafka Runner, that can be used for rendering frames with Blender.

## How to use

The renderer waits for messages from a Kafka topic. When a message is received and it passes validation, a command is constructed based on the contents of the message and the command is executed. This command is used to render a certain amount of frames with Blender from a specified scene. After the rendering is completed, an output message will be sent in a Kafka topic.

The incoming and outgoing messages are validated against the json schemas:
 - **render_job.schema.json**
 - **render_output.schema.json**

See the schema descriptions in the [repository for json schemas](http://10.8.8.219/fungi/json_schemas).

To start the rendering service, start the [kafka_render.py](kafka_render.py) script from the command line like so:
```bash
python kafka_render.py IN.TOPIC.NAME OUT.TOPIC.NAME BOOTSTRAP.SERVER.ADDRESS
```
Where `IN.TOPIC.NAME` should be the name of the Kafka topic for incoming messages (render jobs), `OUT.TOPIC.NAME` should be the name of the Kafka topic for outgoing messages (render outputs), and BOOTSTRAP.SERVER.ADDRESS is the address of the Kafka bootstrap server. In the case when there are multiple bootstrap servers, they should be listed with spaces in between.

An example command-line use, with an input topic name of `render.job`, an output topic name of `render.out` and bootstrap servers 10.8.8.200:9090 and 10.8.8.201:9094 would be
```bash
python kafka_render.py render.job render.out 10.8.8.200:9090 10.8.8.201:9094
```

Apart from these compulsory positional arguments the script can also be used with keyword arguments. These are the following:
 - `-b` or `--blender` can be used to specify the path to the Blender executable (the default value is `blender`)
 - `-g` or `--group_id` can be used to specify the consumer group ID for the Kafka consumer (by default this is not used)
 - `-e` or `--error_topic_name` can be used to specify the Kafka topic in which the error messages will be sent (by default errors will be sent to the `error.log` Kafka topic)
 - `-d` or `--debug` is a flag that can be set to run the script in debug mode

Using the previous example, but with specifying the Blender executable path as `/home/user/blender/blender`, the consumer group ID as `my_consumer_group`, the error topic name as `render.errors` and running the script in debug mode would look like this:
```bash
python kafka_render.py render.job render.out 10.8.8.200:9090 10.8.8.201:9094 -b /home/user/blender/blender -g my_consumer_group -e render.errors -d
```

When the script is up and running you can start sending messages to the specified Kafka topic and wait wait for responses in the output topic. The rendered frames should be uploaded to the Minio object storage at the location specified in the render job.

## Technical details and deployment

The renderer creates its own Minio client, which looks for the credentials in `../credentials`. Before running the renderer you have to make sure, that the credentials file (minio_credentials.json) is available at that location.

The json schemas are searched for at the `../json_schemas` location. Before running the renderer you have to make sure, that the schemas are available at that location. If not, the script will still run, but incoming and outgoing messages will not be validated.

When an incoming Kafka message is received, the requested Blender scene is downloaded from the Minio storage (to the location `scenes`). This **ONLY** happens if there is not a local copy of the scene already!
When the scene is available, the [render_frames.py](render_frames.py) script is used with Blender to render the images. The constructed command has the general form:
```bash
blender scene --background --python render_frames.py -- args_for_render_frames.py
```

The rendered frames are saved at the `tmp_render_out` folder locally. When the whole rendering process is finished, the contents of this folder are uploaded to the requested location in the Minio storage, and the local `tmp_render_out` folder is emptied. The output from the renderer containing the time taken for the rendering process is also sent to the output topic.

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