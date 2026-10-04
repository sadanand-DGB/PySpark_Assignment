from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_date,
    year,
    month,
    dayofmonth
)

from util import (
    read_json_data,
    flatten_struct_columns,
    explode_employees,
    explode_outer_employees,
    posexplode_employees
)


spark = SparkSession.builder \
    .appName("Question 4") \
    .getOrCreate()


# Q4.1 - Read JSON file.
json_file_path = "src/Question_4/nested_json_file.json"

employee_df = read_json_data(
    spark,
    json_file_path
)

print("Q4.1 - Original Data:")
employee_df.show(truncate=False)


# Q4.2 - Flatten struct columns.
df2 = flatten_struct_columns(employee_df)

print("Q4.2 - Flattened Data:")
df2.show(truncate=False)


# Q4.3 - Compare record counts.
original_count = employee_df.count()
flattened_count = df2.count()

print("Q4.3 - Original Count:", original_count)
print("Q4.3 - Flattened Count:", flattened_count)


# Q4.4 - Explode employees.
df3 = explode_employees(df2)

print("Q4.4 - Explode:")
df3.show(truncate=False)


# Explode outer.
df4 = explode_outer_employees(df2)

print("Q4.4 - Explode Outer:")
df4.show(truncate=False)


# Posexplode.
df5 = posexplode_employees(df2)

print("Q4.4 - Posexplode:")
df5.show(truncate=False)


# Flatten employee struct.
df6 = df5.select(
    "id",
    "name",
    "storeSize",
    "position",
    col("employee.empId").alias("empId"),
    col("employee.empName").alias("empName")
)

print("Flattened Employee Data:")
df6.show(truncate=False)


# Q4.5 - Filter id.
df7 = employee_df.filter(
    col("id") == 1001
)

print("Q4.5 - ID = 1001:")
df7.show(truncate=False)


# Q4.6 - Convert camelCase to snake_case.
df8 = df6 \
    .withColumnRenamed("storeSize", "store_size") \
    .withColumnRenamed("empId", "emp_id") \
    .withColumnRenamed("empName", "emp_name")

print("Q4.6 - Snake Case:")
df8.show(truncate=False)


# Q4.7 - Add load_date.
df9 = df8.withColumn(
    "load_date",
    current_date()
)

print("Q4.7 - Load Date:")
df9.show(truncate=False)


# Q4.8 - Create year, month and day.
df10 = df9 \
    .withColumn("year", year("load_date")) \
    .withColumn("month", month("load_date")) \
    .withColumn("day", dayofmonth("load_date"))

print("Q4.8 - Year, Month and Day:")
df10.show(truncate=False)


spark.stop()