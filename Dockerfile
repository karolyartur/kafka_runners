FROM python:3.6-slim as staging
RUN apt update && apt install -y git
WORKDIR /app/
RUN python -m venv /app/venv
# Enable venv
ENV PATH="/app/venv/bin:$PATH"
RUN git clone https://github.com/karolyartur/json_schemas && git clone https://github.com/karolyartur/blender_annotation_tool
COPY . /app/kafka_runners
RUN pip install -Ur kafka_runners/requirements.txt

FROM nytimes/blender:3.1-gpu-ubuntu18.04 as runner
WORKDIR /app/kafka_runners
COPY --from=staging /app /app
# Enable venv
ENV PATH="/app/venv/bin:$PATH"

ENV IN_KAFKA_TOPIC=""
ENV OUT_KAFKA_TOPIC="" 
ENV KAFKA_BROKERS=""

RUN apt-get update && apt-get install -y  \
        python3 \
        xorg  \
        openbox \
        xvfb  \
        libxxf86vm1 \
        libxfixes3 \
        libgl1 && \
    update-alternatives --install /app/venv/bin/python3 python3 /usr/bin/python3 2 &&  \
    mv /app/blender_annotation_tool /bin/3.1/scripts/addons
#    mv xvfb /etc/init.d/ && \
#    chmod +x /etc/init.d/xvfb && \
#    useradd xvfb && \
#    update-rc.d xvfb defaults && \
#    echo "export DISPLAY=:0" >> ~/.bashrc
#CMD cd kafka_runners && python3 kafka_render.py $IN_KAFKA_TOPIC $OUT_KAFKA_TOPIC $KAFKA_BROKERS
# RUN Xvfb :2 -screen 0 1920x1080x24+32 &
# RUN DISPLAY=:2.0 
# RUN export DISPLAY

#ENTRYPOINT python3 kafka_annotate.py $IN_KAFKA_TOPIC $OUT_KAFKA_TOPIC $KAFKA_BROKERS
ENTRYPOINT [ "/bin/sh" ,"kafka-annotate.sh" ]