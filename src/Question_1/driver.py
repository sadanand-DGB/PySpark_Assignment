from pyspark.sql import SparkSession

from src.Question_1.util import (
    create_purchase_data,
    create_product_data
)


# Create Spark session
spark = SparkSession.builder.appName("Question 1").getOrCreate()


# Create DataFrames
purchase_data_df = create_purchase_data(spark)
product_data_df = create_product_data(spark)



# Q1.1


print("Q1.1 - Purchase Data:")
purchase_data_df.show()

print("Q1.1 - Product Data:")
product_data_df.show()



# Q1.2
# Find customers who bought only iphone13


# Count purchases made by each customer
df2 = purchase_data_df.groupBy("customer").count()

print("Q1.2 - Purchase count by customer:")
df2.show()


# Keep customers who made only one purchase
df3 = df2.filter(
    df2["count"] == 1
)

print("Q1.2 - Customers with only one purchase:")
df3.show()


# Keep only iphone13 purchases
df4 = purchase_data_df.filter(
    purchase_data_df["product_model"] == "iphone13"
)

print("Q1.2 - Iphone13 purchases:")
df4.show()


# Create aliases
iphone13_purchases = df4.alias("iphone13_purchases")
single_purchase_customers = df3.alias("single_purchase_customers")


# Find customers who bought only iphone13
df5 = iphone13_purchases.join(
    single_purchase_customers,
    iphone13_purchases.customer == single_purchase_customers.customer,
    "inner"
)

print("Q1.2 - Customers who bought only iphone13:")
df5.select(
    iphone13_purchases.customer
).show()



# Q1.3
# Find customers who upgraded from iphone13 to iphone14


# Get customers who bought iphone13
df6 = purchase_data_df.filter(
    purchase_data_df["product_model"] == "iphone13"
)

print("Q1.3 - Iphone13 customers:")
df6.show()


# Get customers who bought iphone14
df7 = purchase_data_df.filter(
    purchase_data_df["product_model"] == "iphone14"
)

print("Q1.3 - Iphone14 customers:")
df7.show()


# Create aliases
iphone13_customers = df6.alias("iphone13_customers")
iphone14_customers = df7.alias("iphone14_customers")


# Find customers who bought both products
df8 = iphone13_customers.join(
    iphone14_customers,
    iphone13_customers.customer == iphone14_customers.customer,
    "inner"
)

print("Q1.3 - Customers who upgraded from iphone13 to iphone14:")
df8.select(
    iphone13_customers.customer
).show()



# Q1.4
# Find customers who bought all models


# Remove duplicate purchases
df9 = purchase_data_df.distinct()

print("Q1.4 - Distinct purchase data:")
df9.show()


# Count models bought by each customer
df10 = df9.groupBy("customer").count()

print("Q1.4 - Models bought by each customer:")
df10.show()


# Count total models in product data
total_models = product_data_df.count()


# Keep customers who bought all models
df11 = df10.filter(
    df10["count"] == total_models
)

print("Q1.4 - Customers who bought all models:")
df11.select("customer").show()