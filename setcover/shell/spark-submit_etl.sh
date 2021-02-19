#!/bin/sh

spark-submit \
        --class org.apache.spark.examples.SparkPi \
        /opt/spark/examples/src/main/python/pi.py \
        12000