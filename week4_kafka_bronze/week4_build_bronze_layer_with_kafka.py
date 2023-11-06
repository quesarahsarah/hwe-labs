import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

def getScramAuthString(username, password):
  return f"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username="{username}"
   password="{password}";
  """

# Define the Kafka broker and topic to read from
kafka_bootstrap_servers = os.environ.get("HWE_BOOTSTRAP")
username = os.environ.get("HWE_USERNAME")
password = os.environ.get("HWE_PASSWORD")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
kafka_topic = "reviews"

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week4Lab") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()

#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

# Read data from Kafka using the DataFrame API
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "1000") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config", getScramAuthString(username, password)) \
    .load()

# Process the received data


# Modify the `df` dataframe defined in the lab to do the following:

#    * split the value of the Kafka message on tab characters, assigning a field name to each element using the `as` keyword
#    * append a column to the data named `review_timestamp` which is set to the current_timestamp
df2 = df \
    .select(split(col("value").cast("string"), "\t").alias("tsv")) \
    .select(col("tsv").getItem(0).alias("marketplace"), \
            col("tsv").getItem(1).alias("customer_id"), \
            col("tsv").getItem(2).alias("review_id"), \
            col("tsv").getItem(3).alias("product_id"), \
            col("tsv").getItem(4).alias("product_parent"), \
            col("tsv").getItem(5).alias("product_title"), \
            col("tsv").getItem(6).alias("product_category"), \
            col("tsv").getItem(7).cast("int").alias("star_rating"), \
            col("tsv").getItem(8).cast("int").alias("helpful_votes"), \
            col("tsv").getItem(9).cast("int").alias("total_votes"), \
            col("tsv").getItem(10).alias("vine"), \
            col("tsv").getItem(11).alias("verified_purchase"), \
            col("tsv").getItem(12).alias("review_headline"), \
            col("tsv").getItem(13).alias("review_body"), \
            col("tsv").getItem(14).alias("purchase_date")) \
    .withColumn("review_timestamp", current_timestamp())



#    * write that data as Parquet files to S3 under `s3a://hwe-$CLASS/$HANDLE/bronze/reviews` using append mode and a checkpoint location of `/tmp/kafka-checkpoint`
   
query = df2 \
  .writeStream \
  .outputMode("append") \
  .format("parquet") \
  .option("path", "s3a://hwe-fall-2023/sbrown/bronze/reviews") \
  .option("checkpointLocation", "/tmp/kafka-checkpoint") \
  .start()
  

# Wait for the streaming query to finish
query.awaitTermination()

# Stop the SparkSession
spark.stop()