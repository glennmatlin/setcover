#!/bin/sh

spark-submit \
        --class org.apache.spark.examples.SparkPi \
        --conf spark.driver.maxResultSize=20g \
        --conf spark.hadoop.fs.s3a.server-side-encryption-algorithm=SSE-KMS \
        --conf spark.hadoop.fs.s3a.server-side-encryption.key=arn:aws:kms:us-west-2:010539206827:key/53543671-0c18-4dbe-a6af-f006aabff4a4 \
        --driver-memory 20g \
        --conf spark.executor.extraClassPath=${HOME}/${JDBC_DRIVER_JAR} \
        --conf spark.hadoop.fs.s3a.maxRetries=40 \
        --conf spark.task.maxFailures=10 \
        --conf spark.sql.shuffle.partitions=4800 \
        --conf spark.sql.broadcastTimeout=3600 \
        --driver-class-path ${HOME}/${JDBC_DRIVER_JAR} \
        --executor-memory ${SPARK_EXECUTOR_MEMORY:-"${default_executor_ram}G"} \
        --jars ${HOME}/${JDBC_DRIVER_JAR} \
        --master spark://${SPARK_MASTER_DNS}:7077 \
        --py-files /opt/spark/examples/src/main/python/etl.py