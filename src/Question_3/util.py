from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)


def create_login_data(spark):

    # Login activity data

    data = [
        (1, 101, "login", "2023-09-05 08:30:00"),
        (2, 102, "click", "2023-09-06 12:45:00"),
        (3, 101, "click", "2023-09-07 14:15:00"),
        (4, 103, "login", "2023-09-08 09:00:00"),
        (5, 102, "logout", "2023-09-09 17:30:00"),
        (6, 101, "click", "2023-09-10 11:20:00"),
        (7, 103, "click", "2023-09-11 10:15:00"),
        (8, 102, "click", "2023-09-12 13:10:00")
    ]


    # Define custom schema

    schema = StructType([
        StructField("log id", IntegerType(), True),
        StructField("user$id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])


    # Create DataFrame

    login_data_df = spark.createDataFrame(
        data,
        schema
    )

    return login_data_df


def rename_columns_dynamically(df):

    # Rename columns dynamically

    new_column_names = [
        "log_id",
        "user_id",
        "user_activity",
        "time_stamp"
    ]

    for old_name, new_name in zip(
        df.columns,
        new_column_names
    ):

        df = df.withColumnRenamed(
            old_name,
            new_name
        )

    return df