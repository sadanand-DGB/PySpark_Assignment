from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)


def create_purchase_data(spark):

    # Purchase data
    purchase_data = [
        (1, "iphone13"),
        (1, "dell i5 core"),
        (2, "iphone13"),
        (2, "dell i5 core"),
        (3, "iphone13"),
        (3, "dell i5 core"),
        (1, "dell i3 core"),
        (1, "hp i5 core"),
        (1, "iphone14"),
        (3, "iphone14"),
        (4, "iphone13")
    ]

    # Define purchase schema
    purchase_schema = StructType([
        StructField("customer", IntegerType(), True),
        StructField("product_model", StringType(), True)
    ])

    # Create purchase DataFrame
    purchase_data_df = spark.createDataFrame(
        purchase_data,
        purchase_schema
    )

    return purchase_data_df


def create_product_data(spark):

    # Product data
    product_data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",)
    ]

    # Define product schema
    product_schema = StructType([
        StructField("product_model", StringType(), True)
    ])

    # Create product DataFrame
    product_data_df = spark.createDataFrame(
        product_data,
        product_schema
    )

    return product_data_df