FROM python:3.6-slim as staging
RUN apt update && apt install -y git
WORKDIR /app/
RUN python -m venv /app/venv
# Enable venv
ENV PATH="/app/venv/bin:$PATH"
RUN git clone https://github.com/karolyartur/json_schemas
COPY . /app/kafka_runners
RUN pip install -Ur kafka_runners/requirements.txt

FROM nytimes/blender:3.1-cpu-ubuntu18.04 as runner
WORKDIR /app/
COPY --from=staging /app /app
# Enable venv
ENV PATH="/app/venv/bin:$PATH"
RUN apt-get update && apt-get install -y python3 && update-alternatives --install /app/venv/bin/python3 python3 /usr/bin/python3 2
CMD cd kafka_runners && python3 kafka_render.py render.job render.out 10.8.8.226:9092