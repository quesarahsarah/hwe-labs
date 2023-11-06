import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# Load environment variables.
from dotenv import load_dotenv
load_dotenv()

aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Week5Lab") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
    .master('local[*]') \
    .getOrCreate()

# ***** corrected example from the slides — adds [] *****
# from pyspark.sql.types import StructType, StructField, StringType
# player_schema = StructType([
#     StructField("name", StringType, nullable=False)
#     ,StructField("gender", StringType, nullable=False)
#     ,StructField("state", StringType, nullable=False)
# ])
# players = spark.read.schema(player_schema).csv("path")


#For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

# /////////////////////////////////////////////////////////////////////////////////////////////////
# 1. Define a `bronze_schema` which describes the Parquet files under the bronze reviews directory on S3

bronze_schema = StructType([
    StructField("marketplace", StringType(), nullable=False)
    ,StructField("customer_id", StringType(), nullable=False)
    ,StructField("review_id", StringType(), nullable=False)
    ,StructField("product_id", StringType(), nullable=False)
    ,StructField("product_parent", StringType(), nullable=False)
    ,StructField("product_title", StringType(), nullable=False)
    ,StructField("product_category", StringType(), nullable=False)
    ,StructField("star_rating", IntegerType(), nullable=False)
    ,StructField("helpful_votes", IntegerType(), nullable=False)
    ,StructField("total_votes", IntegerType(), nullable=False)
    ,StructField("vine", StringType(), nullable=False)
    ,StructField("verified_purchase", StringType(), nullable=False)
    ,StructField("review_headline", StringType(), nullable=False)
    ,StructField("review_body", StringType(), nullable=False)
    ,StructField("purchase_date", StringType(), nullable=False)
    ,StructField("review_timestamp", TimestampType(), nullable=False)
])

# /////////////////////////////////////////////////////////////////////////////////////////////////
# 2. Define a streaming dataframe using `readStream` on top of the bronze reviews directory on S3

bronze_reviews = spark.readStream.schema(bronze_schema).parquet("s3a://hwe-fall-2023/sbrown/bronze/reviews/")

# /////////////////////////////////////////////////////////////////////////////////////////////////
# 3. Register a virtual view on top of that dataframe

bronze_reviews.createOrReplaceTempView("reviews")
spark.sql("SELECT * FROM reviews").printSchema()

# /////////////////////////////////////////////////////////////////////////////////////////////////
# 4. Define a non-streaming dataframe using `read` on top of the bronze customers directory on S3

bronze_customers = spark.read.parquet("s3a://hwe-fall-2023/sbrown/bronze/customers/")

# /////////////////////////////////////////////////////////////////////////////////////////////////
# 5. Register a virtual view on top of that dataframe

bronze_customers.createOrReplaceTempView("customers")
spark.sql("SELECT * FROM customers").printSchema()

# /////////////////////////////////////////////////////////////////////////////////////////////////
# 6. Define a `silver_data` dataframe by:
#    * joining the review and customer data on their common key of `customer_id`
#    * applying a business validation rule to prevent unverified reviews in the
#    * bronze layer from being loaded into the silver layer

# silver_data = spark.sql("SELECT * \
#                         FROM customers \
#                         NATURAL JOIN reviews \
#                         WHERE reviews.verified_purchase = 'Y' ")

silver_data = spark.sql("SELECT \
                        r.marketplace, \
                        r.customer_id, \
                        r.review_id, \
                        r.product_id, \
                        r.product_parent, \
                        r.product_title, \
                        r.product_category, \
                        r.star_rating, \
                        r.helpful_votes, \
                        r.total_votes, \
                        r.vine, \
                        r.verified_purchase, \
                        r.review_headline, \
                        r.review_body, \
                        CAST (r.purchase_date AS date) AS purchase_date, \
                        r.review_timestamp, \
                        c.customer_name, \
                        c.gender, \
                        CAST (c.date_of_birth AS date) AS date_of_birth, \
                        c.city, \
                        c.state \
                        FROM reviews r \
                        INNER JOIN customers c \
                        ON r.customer_id = c.customer_id \
                        AND r.verified_purchase = 'Y' ")
silver_data.printSchema()

# /////////////////////////////////////////////////////////////////////////////////////////////////
# 7. Write that silver data to S3 under `s3a://hwe-$CLASS/$HANDLE/silver/reviews` using append mode,
#    a checkpoint location of `/tmp/silver-checkpoint`, and a format of `parquet`
streaming_query = silver_data \
  .writeStream \
  .outputMode("append") \
  .format("parquet") \
  .option("path", "s3a://hwe-fall-2023/sbrown/silver/reviews") \
  .option("checkpointLocation", "/tmp/silver-checkpoint") \


streaming_query.start().awaitTermination()

## Stop the SparkSession
spark.stop()
