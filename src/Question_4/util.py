from pyspark.sql.functions import explode, explode_outer, posexplode


def read_json_data(spark, file_path):
    # Q4.1 - Read multiline JSON file.
    return spark.read \
        .option("multiline", True) \
        .json(file_path)


def flatten_struct_columns(df):
    # Q4.2 - Flatten struct columns.
    return df.select(
        "id",
        "properties.name",
        "properties.storeSize",
        "employees"
    )


def explode_employees(df):
    # Q4.4 - Explode employees array.
    return df.select(
        "id",
        "name",
        "storeSize",
        explode("employees").alias("employee")
    )


def explode_outer_employees(df):
    # Q4.4 - Explode outer employees array.
    return df.select(
        "id",
        "name",
        "storeSize",
        explode_outer("employees").alias("employee")
    )


def posexplode_employees(df):
    # Q4.4 - Posexplode employees array.
    return df.select(
        "id",
        "name",
        "storeSize",
        posexplode("employees").alias("position", "employee")
    )