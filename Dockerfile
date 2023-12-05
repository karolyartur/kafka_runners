FROM python:3.10-slim as staging
RUN apt-get update && apt-get install -y git
WORKDIR /app/
RUN git clone https://github.com/karolyartur/json_schemas
COPY . /app/kafka_runners

FROM python:3.10-slim as runner
WORKDIR /app/kafka_runners
COPY --from=staging /app /app

RUN pip install -Ur /app/kafka_runners/requirements.txt

ENV IN_KAFKA_TOPIC=""
ENV OUT_KAFKA_TOPIC=""
ENV KAFKA_BROKERS=""
ENV DB_USER=""
ENV DB_PASS = ""
ENV DB_IP = ""
ENV DB_NAME = ""
ENV GROUP_ID = ""

ENTRYPOINT python3 kafka_track.py $IN_KAFKA_TOPIC $OUT_KAFKA_TOPIC $KAFKA_BROKERS --db_user $DB_USER --db_pass $DB_PASS --db_ip $DB_IP --db_name $DB_NAME --group_id $GROUP_ID