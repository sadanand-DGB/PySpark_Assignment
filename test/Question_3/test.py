from src.Question_3.driver import (
    login_data_df,
    df2,
    df5,
    df6
)


# Question 3.1
# Test whether the login DataFrame contains 8 records

def test_login_data():
    assert login_data_df.count() == 8



# Question 3.2
# Test whether the columns were renamed correctly

def test_column_names():
    assert df2.columns == [
        "log_id",
        "user_id",
        "user_activity",
        "time_stamp"
    ]



# Question 3.3
# Test whether the action count DataFrame contains users

def test_user_action_count():
    assert df5.count() > 0



# Question 3.4
# Test whether login_date column was created

def test_login_date():
    assert "login_date" in df6.columns
    assert df6.select("login_date").first()[0] is not None