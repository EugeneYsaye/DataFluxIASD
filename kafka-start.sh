#!/bin/bash
echo "Starting Kafka broker..."
docker rm -f kafka-broker
docker network create teaching
docker run docker run -d --network teaching --name kafka-proxy alpine/socat \
  tcp-listen:9092,fork,reuseaddr tcp-connect:kafka-broker:9092 \
  -it --rm --name kafka-broker --network teaching --user root -p 9000:9000 -p 9092:9092 ghcr.io/osekoo/kafka:3.5
