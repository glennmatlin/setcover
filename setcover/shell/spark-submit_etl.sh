#!/bin/sh

spark-submit \
        --class setcover.etl \
        /home/jovyan/dev/setcover/etl.py \
        12000