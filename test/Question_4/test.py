from src.Question_4.driver import (
    employee_df,
    df2,
    df3,
    df4,
    df5,
    df6,
    df7,
    df8,
    df9,
    df10
)


# Question 4.1
# Test whether the JSON DataFrame contains one company record

def test_employee_data():
    assert employee_df.count() == 1


# Question 4.2
# Test whether the nested properties and employees columns exist

def test_nested_columns():
    assert "properties" in employee_df.columns
    assert "employees" in employee_df.columns


# Question 4.3
# Test whether the properties struct was flattened correctly

def test_struct_flattening():
    assert df2.columns == [
        "id",
        "name",
        "storeSize",
        "employees"
    ]


# Question 4.5
# Test whether explode creates three employee records

def test_explode():
    assert df3.count() == 3


# Question 4.6
# Test whether explode_outer also returns the three employees

def test_explode_outer():
    assert df4.count() == 3


# Question 4.7
# Test whether posexplode creates three records with positions

def test_posexplode():
    assert df5.count() == 3
    assert "position" in df5.columns


# Question 4.8
# Test whether the employee struct was flattened

def test_employee_flattening():
    assert "empId" in df6.columns
    assert "empName" in df6.columns
    assert df6.count() == 3


# Question 4.9
# Test whether id 1001 is returned from the supplied JSON data

def test_filter_id():
    assert df7.count() == 1
    assert df7.first()["id"] == 1001


# Question 4.10
# Test whether camelCase columns were converted to snake_case

def test_snake_case_columns():
    assert "store_size" in df8.columns
    assert "emp_id" in df8.columns
    assert "emp_name" in df8.columns


# Question 4.11
# Test whether load_date was created

def test_load_date():
    assert "load_date" in df9.columns
    assert df9.select("load_date").first()[0] is not None


# Question 4.12
# Test whether year, month and day columns were created

def test_date_columns():
    assert "year" in df10.columns
    assert "month" in df10.columns
    assert "day" in df10.columns