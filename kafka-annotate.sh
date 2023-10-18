#!/bin/sh
Xvfb :2 -screen 0 1920x1080x16 &
export DISPLAY=:2.0 

python3 kafka_annotate.py $IN_KAFKA_TOPIC $OUT_KAFKA_TOPIC $KAFKA_BROKERS