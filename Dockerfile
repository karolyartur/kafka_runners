FROM python:3.10-slim as staging
RUN apt-get update && apt-get install -y git
WORKDIR /app/
RUN git clone https://github.com/karolyartur/json_schemas && git clone https://github.com/karolyartur/vision
COPY . /app/kafka_runners

FROM pytorch/pytorch:2.0.0-cuda11.7-cudnn8-runtime as runner
WORKDIR /app/kafka_runners
COPY --from=staging /app /app

RUN apt-get update && apt-get install -y gcc && pip install -Ur /app/kafka_runners/requirements.txt && pip install --upgrade requests

ENV PYTHONPATH="$PYTHONPATH:/app/vision/references/detection"

ENV IN_KAFKA_TOPIC=""
ENV OUT_KAFKA_TOPIC=""
ENV KAFKA_BROKERS=""
ENV DB_USER=""
ENV DB_PASS = ""
ENV DB_IP = ""
ENV DB_NAME = ""

ENTRYPOINT python3 kafka_infer.py $IN_KAFKA_TOPIC $OUT_KAFKA_TOPIC $KAFKA_BROKERS --db_user $DB_USER --db_pass $DB_PASS --db_ip $DB_IP --db_name $DB_NAME
