from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


def create_credit_card_data(spark):

    # Credit card data
    card_data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]

    # Define credit card schema
    card_schema = StructType([
        StructField("card_number", StringType(), True)
    ])

    # Create credit card DataFrame
    credit_card_df = spark.createDataFrame(
        card_data,
        card_schema
    )

    return credit_card_df