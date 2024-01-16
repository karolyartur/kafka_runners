FROM python:3.10-slim as staging
RUN apt-get update && apt-get install -y git
WORKDIR /app/
RUN git clone https://github.com/karolyartur/json_schemas
COPY . /app/kafka_runners

FROM python:3.10-slim as runner
WORKDIR /app/kafka_runners
COPY --from=staging /app /app

RUN apt-get update && apt-get install -y gcc && pip install -Ur /app/kafka_runners/requirements.txt && pip install --upgrade requests

ENV IN_KAFKA_TOPIC=""
ENV OUT_KAFKA_TOPIC=""
ENV KAFKA_BROKERS=""
ENV WEIGHTS_FILE=""

ENTRYPOINT python3 kafka_minio_trigger.py $IN_KAFKA_TOPIC $OUT_KAFKA_TOPIC $KAFKA_BROKERS --group_id $MINIO_TRIGGER_GROUP_ID --weights_file $WEIGHTS_FILE --put_to_db

