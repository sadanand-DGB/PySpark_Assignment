from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)


def create_employee_data(spark):
    # Q5.1 - Create employee DataFrame with a custom schema.

    data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]

    schema = StructType([
        StructField("employee id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("State", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("Age", IntegerType(), True)
    ])

    return spark.createDataFrame(data, schema)


def create_department_data(spark):
    # Q5.1 - Create department DataFrame with a custom schema.

    data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]

    schema = StructType([
        StructField("dept_id", StringType(), True),
        StructField("dept_name", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


def create_country_data(spark):
    # Q5.1 - Create country DataFrame with a custom schema.

    data = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]

    schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])

    return spark.createDataFrame(data, schema)


def rename_columns_dynamically(df):
    # Rename employee id dynamically to employee_id.

    for column_name in df.columns:

        new_name = column_name.replace(" ", "_")

        if column_name != new_name:
            df = df.withColumnRenamed(
                column_name,
                new_name
            )

    return df


def lowercase_columns_dynamically(df):
    # Convert all column names to lowercase dynamically.

    for column_name in df.columns:

        df = df.withColumnRenamed(
            column_name,
            column_name.lower()
        )

    return df