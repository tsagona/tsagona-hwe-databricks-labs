-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Week 2 Lab: Query with SQL (Databricks) - CSV Version

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 1
-- MAGIC Read the CSV file into a view called "reviews".
-- MAGIC
-- MAGIC The file should be uploaded to: Workspace > your user folder > reviews.csv

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW reviews AS
SELECT * FROM read_files(
  '/Workspace/Users/${current_user()}/reviews.csv',
  format => 'csv',
  header => true,
  delimiter => '\t'
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 2
-- MAGIC Create a virtual view on top of the reviews dataframe, so that we can query it with Spark SQL.
-- MAGIC
-- MAGIC (Already done above!)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 3
-- MAGIC Add a column to the dataframe named "review_timestamp", representing the current time on your computer.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW with_review_timestamp AS
SELECT *, current_timestamp() AS review_timestamp FROM reviews

-- COMMAND ----------

SELECT * FROM with_review_timestamp LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 4
-- MAGIC How many records are in the reviews dataframe?

-- COMMAND ----------

SELECT count(*) FROM reviews

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 5
-- MAGIC Print the first 5 rows of the dataframe.
-- MAGIC Some of the columns are long - print the entire record, regardless of length.

-- COMMAND ----------

SELECT * FROM reviews LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 6
-- MAGIC Create a new dataframe based on "reviews" with exactly 1 column: the value of the product category field.
-- MAGIC Look at the first 50 rows of that dataframe.
-- MAGIC Which value appears to be the most common?

-- COMMAND ----------

SELECT product_category FROM reviews LIMIT 50

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 7
-- MAGIC Find the most helpful review in the dataframe - the one with the highest number of helpful votes.
-- MAGIC What is the product title for that review? How many helpful votes did it have?

-- COMMAND ----------

SELECT product_title, cast(helpful_votes as int)
FROM reviews
ORDER BY helpful_votes DESC
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 8
-- MAGIC How many reviews exist in the dataframe with a 5 star rating?

-- COMMAND ----------

SELECT count(*) FROM reviews WHERE star_rating = "5"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 9
-- MAGIC Currently every field in the data file is interpreted as a string, but there are 3 that should really be numbers.
-- MAGIC Create a new dataframe with just those 3 columns, except cast them as "int"s.
-- MAGIC Look at 10 rows from this dataframe.

-- COMMAND ----------

SELECT
  cast(star_rating as int),
  cast(helpful_votes as int),
  cast(total_votes as int)
FROM reviews
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 10
-- MAGIC Find the date with the most purchases.
-- MAGIC Print the date and total count of the date which had the most purchases.

-- COMMAND ----------

SELECT purchase_date, count(*) AS purchase_count
FROM reviews
GROUP BY purchase_date
ORDER BY purchase_count DESC
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Question 11
-- MAGIC Write the dataframe from Question 3 to your drive in JSON format.
-- MAGIC Use overwrite mode.
-- MAGIC
-- MAGIC (This requires Python for the write operation.)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.table("with_review_timestamp").write \
-- MAGIC .mode("overwrite") \
-- MAGIC .json("/tmp/reviews_json")
