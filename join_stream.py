import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.streaming.state import GroupStateTimeout
from datetime import datetime
from typing import Iterator, Tuple

import logging

# Set up logging to console only
logging.basicConfig(
    level=logging.INFO,  # Set log level
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    handlers=[
        logging.StreamHandler()  # Log to the console only
    ]
)

logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StatefulOrdersPaymentsStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "2") \
    .config("spark.default.parallelism", "2") \
    .getOrCreate()

# Define schemas for orders and payments
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", IntegerType(), True)
])

payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_date", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("amount", IntegerType(), True)
])

# Read orders stream
orders_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092") \
    .option("subscribe", "orders_topic_data_v1") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='U4ZHZEMXRQQEL62M' password='9fBSku8MpYbWKI46BEqBxbR4h1IWgauihDZnHmma5HxdLCG0CbIXMbt0njnqcIoC';") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value") \
    .select(from_json(col("value"), order_schema).alias("data")) \
    .select("data.*") \
    .withColumn("type", lit("order"))

logger.info("Order Read Stream Started.........")

# Read payments stream
payments_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092") \
    .option("subscribe", "payments_topic_data_v1") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='U4ZHZEMXRQQEL62M' password='9fBSku8MpYbWKI46BEqBxbR4h1IWgauihDZnHmma5HxdLCG0CbIXMbt0njnqcIoC';") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value") \
    .select(from_json(col("value"), payment_schema).alias("data")) \
    .select("data.*") \
    .withColumn("type", lit("payment"))

logger.info("Payment Read Stream Started.........")

# Combine streams
combined_stream = orders_stream.unionByName(payments_stream, allowMissingColumns=True)

def process_stateful(
    key: Tuple[str], pdfs: Iterator[pd.DataFrame], state
) -> Iterator[pd.DataFrame]:
    """
    Processes grouped data for a given order_id with stateful logic.

    Args:
        key: The `order_id` as a tuple.
        pdfs: Iterator of pandas DataFrames for the grouped records.
        state: GroupState object for the current key.

    Returns:
        Iterator of pandas DataFrames with joined results or state expiry.
    """

    # Unpack the key
    (order_id,) = key
    logger.info(f"Current Key for process {key}")

    # Get the current state or initialize
    if state.exists:
        (order_date, created_at, customer_id, order_amount) = state.get
    else:
        order_date, created_at, customer_id, order_amount = None, None, None, None

    output_data = []

    logger.info(f"Iterable dataframes for process {pdfs}")

    # Iterate through the PDFs (grouped dataframes)
    for pdf in pdfs:
        for _, row in pdf.iterrows():
            if row["type"] == "order":

                if state.exists:
                    # If state exists, log and ignore the duplicate order
                    logger.warning(f"Duplicate order received for order_id={order_id}. Ignoring.")
                    continue  # Skip processing this duplicate order

                # Update the state with all relevant order details
                
                order_date = row["order_date"]
                created_at = row["created_at"]
                customer_id = row["customer_id"]
                order_amount = row["amount"]

                order_tup = (order_date, created_at, customer_id, order_amount)
                logger.info(f"Processing order event {order_tup}")

                state.update((order_date, created_at, customer_id, order_amount))
                state.setTimeoutDuration(15 * 60 * 1000)  # Set timeout for 15 minutes

            elif row["type"] == "payment":

                if not state.exists:
                    # If no corresponding order, log and ignore the payment
                    logger.warning(f"Payment received for unknown order_id={order_id}. Ignoring.")
                    continue

                # If payment and state exists, join with the state
                payment_date = row["payment_date"]
                payment_amount = row["amount"]
                payment_id = row["payment_id"]
                payment_tup = (payment_date, payment_amount, payment_id)
                logger.info(f"Processing payment event {payment_tup}")

                output_data.append({
                    "order_id": order_id,
                    "order_date": order_date,
                    "created_at": created_at,
                    "customer_id": customer_id,
                    "order_amount": order_amount,
                    "payment_id": payment_id,
                    "payment_date": payment_date,
                    "payment_amount": payment_amount
                })


                # Remove the state after successful join
                logger.info(f"Removing state for order_id={order_id} after successful join.")
                state.remove()

    # Handle state timeout
    if state.hasTimedOut:
        logger.warning(f"State for order_id={order_id} has timed out. Cleaning up.")
        state.remove()

    # Return as an iterator of pandas DataFrames
    return iter([pd.DataFrame(output_data)]) if output_data else iter([])
                              

# Apply stateful processing with applyInPandasWithState
stateful_query = combined_stream.groupBy("order_id").applyInPandasWithState(
    func=process_stateful,
    outputStructType="""
        order_id STRING,
        order_date STRING,
        created_at STRING,
        customer_id STRING,
        order_amount INT,
        payment_id STRING,
        payment_date STRING,
        payment_amount INT
    """,
    stateStructType="""
        order_date STRING,
        created_at STRING,
        customer_id STRING,
        amount INT
    """,
    outputMode="append",
    timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
)

logger.info("State update query started.........")

# Write the output to console for testing
# query = stateful_query.writeStream \
#     .outputMode("update") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

checkpoint_dir = "gs://streaming_checkpointing/kafka_stream_checkpoint"

query = stateful_query.writeStream \
    .outputMode("append") \
    .format("mongodb") \
    .option('spark.mongodb.connection.uri', "mongodb+srv://shashankuser:GrowDataSkills219@mongo-db-cluster.0iwho.mongodb.net/?retryWrites=true&w=majority&appName=mongo-db-cluster") \
    .option('spark.mongodb.database', 'ecomm_mart') \
    .option('spark.mongodb.collection', 'orders_data_process_fact') \
    .option("truncate", False) \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

logger.info("Mongo Write Stream Started.........")

query.awaitTermination()