from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    current_date
)

from util import (
    create_employee_data,
    create_department_data,
    create_country_data,
    rename_columns_dynamically,
    lowercase_columns_dynamically
)


# Create Spark session.

spark = SparkSession.builder \
    .appName("Question 5") \
    .getOrCreate()


# ============================================================
# Q5.1
# Create all three DataFrames with custom schemas.
# ============================================================

employee_df = create_employee_data(spark)

department_df = create_department_data(spark)

country_df = create_country_data(spark)


print("Q5.1 - Employee Data:")
employee_df.show()


print("Q5.1 - Department Data:")
department_df.show()


print("Q5.1 - Country Data:")
country_df.show()


# Rename "employee id" to "employee_id"
# for the remaining questions.

employee_df = rename_columns_dynamically(
    employee_df
)


# ============================================================
# Q5.2
# Find the average salary of each department.
# ============================================================

df2 = employee_df.groupBy(
    "department"
).agg(
    avg("salary").alias("average_salary")
)


print("Q5.2 - Average Salary of Each Department:")
df2.show()


# ============================================================
# Q5.3
# Find employee name and department name
# whose name starts with 'm'.
# ============================================================

df3 = employee_df.join(
    department_df,
    employee_df["department"] == department_df["dept_id"],
    "inner"
).filter(
    (col("employee_name").startswith("m")) |
    (col("dept_name").startswith("m"))
).select(
    "employee_name",
    "dept_name"
)


print("Q5.3 - Employee and Department Names Starting with 'm':")
df3.show()


# ============================================================
# Q5.4
# Create a bonus column by multiplying salary by 2.
# ============================================================

df4 = employee_df.withColumn(
    "bonus",
    col("salary") * 2
)


print("Q5.4 - Employee Data with Bonus:")
df4.show()


# ============================================================
# Q5.5
# Reorder the columns as specified in the assignment.
# ============================================================

df5 = df4.select(
    "employee_id",
    "employee_name",
    "salary",
    "State",
    "Age",
    "department"
)


print("Q5.5 - Reordered Employee Data:")
df5.show()


# ============================================================
# Q5.6
# Perform inner, left and right joins dynamically.
# ============================================================

employee_join_column = "department"

department_join_column = "dept_id"


# Inner join

df6_inner = df5.join(
    department_df,
    df5[employee_join_column] ==
    department_df[department_join_column],
    "inner"
)


print("Q5.6 - Inner Join:")
df6_inner.show()


# Left join

df6_left = df5.join(
    department_df,
    df5[employee_join_column] ==
    department_df[department_join_column],
    "left"
)


print("Q5.6 - Left Join:")
df6_left.show()


# Right join

df6_right = df5.join(
    department_df,
    df5[employee_join_column] ==
    department_df[department_join_column],
    "right"
)


print("Q5.6 - Right Join:")
df6_right.show()


# ============================================================
# Q5.7
# Derive a DataFrame with country_name instead of State.
# ============================================================

df7 = df5.join(
    country_df,
    df5["State"] == country_df["country_code"],
    "left"
).drop(
    "State",
    "country_code"
)


print("Q5.7 - Employee Data with Country Name:")
df7.show()


# ============================================================
# Q5.8
# Convert all column names to lowercase dynamically
# and add the load_date column.
# ============================================================

df8 = lowercase_columns_dynamically(
    df7
)


df8 = df8.withColumn(
    "load_date",
    current_date()
)


print("Q5.8 - Lowercase Columns with Load Date:")
df8.show()


print("Q5.8 - Final Column Names:")
print(df8.columns)


# ============================================================
# Q5.9
# Create two external tables:
# 1. Parquet table
# 2. CSV table
# ============================================================

spark.sql("""
CREATE DATABASE IF NOT EXISTS employee
""")


# Q5.9 - Write the DataFrame as an external Parquet table.

df8.write \
    .format("parquet") \
    .mode("overwrite") \
    .option(
        "path",
        "/Volumes/dev_catalog/default/q5_output/employee_parquet"
    ) \
    .saveAsTable(
        "employee.employee_parquet"
    )


# Q5.9 - Write the DataFrame as an external CSV table.

df8.write \
    .format("csv") \
    .mode("overwrite") \
    .option(
        "header",
        True
    ) \
    .option(
        "path",
        "/Volumes/dev_catalog/default/q5_output/employee_csv"
    ) \
    .saveAsTable(
        "employee.employee_csv"
    )


print("Q5.9 - External tables created:")

print("employee.employee_parquet")

print("employee.employee_csv")


# Stop Spark.

spark.stop()