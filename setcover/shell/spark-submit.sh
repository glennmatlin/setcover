#!/bin/sh

spark-submit \
        --class setcover.etl \
        /home/jovyan/dev/setcover/etl.py \
        12000

spark-submit \
        --class setcover.run \
        /home/jovyan/dev/setcover/run.py \
        12000