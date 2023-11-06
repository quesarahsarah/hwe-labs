from pyspark.sql import SparkSession

### Setup: Create a SparkSession
spark = None

# For Windows users, quiet errors about not being able to delete temporary directories which make your logs impossible to read...
logger = spark.sparkContext._jvm.org.apache.log4j
logger.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager"). setLevel( logger.Level.OFF )
logger.LogManager.getLogger("org.apache.spark.SparkEnv"). setLevel( logger.Level.ERROR )

### Questions

## Question 1: Read the tab separated file named "resources/reviews.tsv.gz" into a dataframe. Call it "reviews".
reviews = spark.read.csv("resources/reviews.tsv.gz", sep="\t", header=True)
# -------------------------------------------------
# Question 2: Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.
reviews.createOrReplaceTempView("reviews_view")
# -------------------------------------------------
# Question 3: Add a column to the dataframe named "review_timestamp", representing the current time on your computer. 
print("\n\nQuestion 3: Add a column to the dataframe named 'review_timestamp', representing the current time on your computer.")


reviews_with_timestamp = spark.sql("SELECT *, current_timestamp() as review_timestamp FROM reviews_view")
reviews_with_timestamp.show(n=5)
# -------------------------------------------------
# Question 4: How many records are in the reviews dataframe? 
print("\n\nQuestion 4: How many records are in the reviews dataframe?")
count_reviews = spark.sql("SELECT COUNT(*) FROM reviews_view")
count_reviews.show()
# answer = 145431



# -------------------------------------------------
# Question 5: Print the first 5 rows of the dataframe. 
# Some of the columns are long - print the entire record, regardless of length.
print("\n\nQuestion 5: Print the first 5 rows of the dataframe. Print the entire record, regardless of length.")

reviews.show(n=5,truncate=False)

# -------------------------------------------------
# Question 6: Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
# Look at the first 50 rows of that dataframe. 
# Which value appears to be the most common?
print("\n\nQuestion 6: Create a new dataframe based on 'reviews' with exactly 1 column: the value of the product category field.")
spark.sql("SELECT product_category FROM reviews_view").show(n=50)
print("Product Category Field that appears to be most common: Digital_Video_Games")

# -------------------------------------------------
# Question 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
# What is the product title for that review? How many helpful votes did it have?
print("\n\nQuestion 7: Find the most helpful review in the dataframe - the one with the highest number of helpful votes. What is the product title for that review? How many helpful votes did it have?")
most_helpful_review = spark.sql("SELECT product_title, helpful_votes FROM reviews_view ORDER BY cast(helpful_votes as int) DESC")
most_helpful_review.show(n=1, truncate=False)

# -------------------------------------------------
# Question 8: How many reviews exist in the dataframe with a 5 star rating?
print("\n\nQuestion 8: How many reviews exist in the dataframe with a 5 star rating?")
spark.sql("SELECT COUNT(*) FROM reviews_view WHERE star_rating = 5").show()
print("80677 reviews have a 5 star rating")

# Question 9: Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
# Create a new dataframe with just those 3 columns, except cast them as "int"s.
cast_reviews = spark.sql("SELECT cast(star_rating as int), cast(helpful_votes as int), cast(total_votes as int) FROM reviews_view")

# Look at 10 rows from this dataframe.

cast_reviews.show(n=10)


# Question 10: Find the date with the most purchases.
# Print the date and total count of the date which had the most purchases.
spark.sql("SELECT purchase_date, COUNT (purchase_date) AS `value_occurrence` FROM reviews_view GROUP BY purchase_date ORDER BY `value_occurrence` DESC LIMIT 1").show()

##Question 11: Write the dataframe from Question 3 to your drive in JSON format.
##Feel free to pick any directory on your computer.
##Use overwrite mode.
reviews_with_timestamp.write.mode("overwrite").json("resources/reviews_json")


### Teardown
# Stop the SparkSession
