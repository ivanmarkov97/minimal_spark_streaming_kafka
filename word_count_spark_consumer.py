import os
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# spark-submit ${PYSPARK_SUBMIT_ARGS} word_count_spark_consumer.py
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1'


def my_udf(encoded_json):
    print(encoded_json)
    json_object = json.loads(encoded_json)['message']
    return json_object


if __name__ == '__main__':
    spark = SparkSession.builder.appName('StreamingApp').master('local[*]').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    decoding_udf = udf(my_udf, StringType())

    df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', 'my-topic') \
        .load()

    result = df.select(decoding_udf('value'))
    result = result.writeStream.format('console').start()

    df.printSchema()
    result.awaitTermination()
