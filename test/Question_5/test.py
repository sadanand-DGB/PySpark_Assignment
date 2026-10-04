from pyspark.sql.functions import col
from src.Question_5 import driver


# Question 5.1
# Test employee data

def test_employee_data():
    assert driver.employee_df.count() == 5


# Question 5.1
# Test department data

def test_department_data():
    assert driver.department_df.count() == 3


# Question 5.1
# Test country data

def test_country_data():
    assert driver.country_df.count() == 3


# Question 5.2
# Test dynamic column names

def test_dynamic_column_names():
    assert driver.df2.columns == [
        "employee_id",
        "employee_name",
        "department_id",
        "salary",
        "country_id"
    ]


# Question 5.3
# Test average salary

def test_average_salary():
    assert driver.df3.first()["average_salary"] == 60000.0


# Question 5.4
# Test employee names starting with M

def test_employee_name_starts_with_m():
    assert driver.df4.count() == 3


# Question 5.4
# Test department names starting with M

def test_department_name_starts_with_m():
    assert driver.df5.count() == 1


# Question 5.5
# Test bonus calculation

def test_bonus():
    result = driver.df6.filter(
        col("employee_id") == 1
    ).first()

    assert result["bonus"] == 100000.0


# Question 5.6
# Test column order

def test_column_order():
    assert driver.df7.columns == [
        "employee_id",
        "employee_name",
        "department_id",
        "country_id",
        "salary",
        "bonus"
    ]


# Question 5.7
# Test inner join

def test_inner_join():
    assert driver.df8.count() == 5


# Question 5.8
# Test left join

def test_left_join():
    assert driver.df9.count() == 5


# Question 5.9
# Test right join

def test_right_join():
    assert driver.df10.count() == 5


# Question 5.10
# Test country join

def test_country_join():
    assert "country_name" in driver.df11.columns


# Question 5.12
# Test lowercase conversion

def test_lowercase():
    result = driver.df13.first()["employee_name"]

    assert result == "mohan"


# Question 5.13
# Test load_date

def test_load_date():
    assert "load_date" in driver.df14.columns