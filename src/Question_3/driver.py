from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    date_sub,
    current_date,
    count
)

from util import (
    create_login_data,
    rename_columns_dynamically
)


# Create Spark session

spark = SparkSession.builder.appName("Question 3").getOrCreate()



# Q3.1
# Create DataFrame with custom schema

login_data_df = create_login_data(spark)

print("Q3.1 - Login Data:")
login_data_df.show(truncate=False)



# Q3.2
# Rename columns dynamically

df2 = rename_columns_dynamically(login_data_df)

print("Q3.2 - DataFrame with renamed columns:")
df2.show(truncate=False)



# Q3.3
# Calculate the number of actions performed by each user in the last 7 days

df3 = df2.withColumn(
    "time_stamp",
    to_timestamp(
        col("time_stamp"),
        "yyyy-MM-dd HH:mm:ss"
    )
)

df4 = df3.filter(
    col("time_stamp") >= date_sub(current_date(), 7)
)

df5 = df4.groupBy(
    "user_id"
).agg(
    count("*").alias("action_count")
)

print("Q3.3 - Actions performed by each user in the last 7 days:")
df5.show()



# Q3.4
# Convert time_stamp to login_date with YYYY-MM-DD format

df6 = df3.withColumn(
    "login_date",
    to_date(col("time_stamp"))
)

print("Q3.4 - Login date:")
df6.select(
    "log_id",
    "user_id",
    "user_activity",
    "login_date"
).show()



# Q3.5
# Write the DataFrame as a CSV file using write options

df6.write \
    .mode("overwrite") \
    .option("header", True) \
    .option("delimiter", ",") \
    .csv("output/question_3_login_details")



# Q3.6
# Create database and write DataFrame as a managed table

spark.sql("CREATE DATABASE IF NOT EXISTS user")

df6.write \
    .mode("overwrite") \
    .saveAsTable("user.login_details")