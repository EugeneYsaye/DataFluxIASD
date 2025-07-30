#!/bin/bash
spark-submit \
    --deploy-mode client \
    --master "spark://localhost:7077" \
    --executor-cores 4 \
    --executor-memory 2G \
    --num-executors 1 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.4 \
    --class "MainApp" \
    "target/scala-2.12/spark-word-count-streaming_2.12-0.1.jar" \