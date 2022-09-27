import logging
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, TimestampType)
import pyspark.sql.functions as psf

from conf import (BROKER_URL, TOPIC_NAME, RADIO_CODE_FILE)

schema = StructType([
    StructField('crime_id', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', StringType(), True),
    StructField('call_date', StringType(), True),
    StructField('offense_date', StringType(), True),
    StructField('call_time', StringType(), True),
    StructField('call_date_time', StringType(), True),
    StructField('disposition', StringType(), True),
    StructField('address', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('common_location', StringType(), True)
])


def run_spark_job(spark):

    df = (
        spark.readStream
            .format('kafka')
            .option("kafka.bootstrap.servers", BROKER_URL)
            .option("subscribe", TOPIC_NAME)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 200)
            .option("stopGracefullyOnShutdown", "true")
            .load()
    )
 logger.info("...Show schema for the df resource for check")
    df.printSchema()


    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = (
        kafka_df
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))
        .select("DF.*"))

    logger.info("...Show schema for the service_table resource for check")
    service_table.printSchema()


    distinct_table =service_table.select(
                psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time"),
                psf.col('original_crime_type_name'),
                psf.col('disposition')
    )

    logger.info("...Show schema for the distinct_table resource for check")
    distinct_table.printSchema()

    agg_df = (
        distinct_table
        .withWatermark('call_date_time', '60 minutes')
            .groupby(
                [psf.window('call_date_time', '15 minutes'),
                 'disposition',
                 'original_crime_type_name']
            )
            .count()
    )

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = (
        agg_df.writeStream
            .outputMode('Complete')
            .format('console')
            .start()
    )

    radio_code_json_filepath = RADIO_CODE_FILE
    radio_code_df = spark.read.option("multiline",True).json(radio_code_json_filepath)
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    logger.info("...Show schema for the radio_code_df resource for check")
    radio_code_df.printSchema()

    join_query = (
        agg_df.join(radio_code_df, 'disposition')
            .writeStream
            .outputMode('Complete')
            .format("console")
            .start()
    )

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    spark = (
        SparkSession.builder
            .config("spark.streaming.kafka.maxRatePerPartition", 50)
            .config("spark.streaming.kafka.processedRowsPerSecond", 50)
            .config("spark.streaming.backpressure.enabled", "true")
            .master("local[*]")
            .appName("KafkaSparkStructuredStreaming")
            .getOrCreate()
            )
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()