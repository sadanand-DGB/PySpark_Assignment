from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from util import create_credit_card_data


# Create Spark session

spark = SparkSession.builder.appName("Question 2").getOrCreate()



# Q2.1
# Create credit card DataFrame

credit_card_df = create_credit_card_data(spark)

print("Q2.1 - Credit Card Data:")

credit_card_df.show(truncate=False)



# Q2.2
# Print the number of partitions

original_partitions = credit_card_df.rdd.getNumPartitions()

print("Q2.2 - Original number of partitions:")

print(original_partitions)



# Q2.3
# Increase the number of partitions to 5

df2 = credit_card_df.repartition(5)

print("Q2.3 - Number of partitions after increasing:")

print(df2.rdd.getNumPartitions())



# Q2.4
# Decrease the number of partitions to 5

df3 = df2.coalesce(5)

print("Q2.4 - Number of partitions after decreasing:")

print(df3.rdd.getNumPartitions())



# Q2.5
# Mask the card number except for the last 4 digits

def mask_card_number(card_number):

    return "*" * (len(card_number) - 4) + card_number[-4:]


# Create UDF

mask_card_udf = udf(
    mask_card_number,
    StringType()
)


# Create masked card number column

df4 = credit_card_df.withColumn(
    "masked_card_number",
    mask_card_udf(credit_card_df["card_number"])
)


# Display card number and masked card number

print("Q2.5 - Masked Credit Card Data:")

df4.select(
    "card_number",
    "masked_card_number"
).show(truncate=False)