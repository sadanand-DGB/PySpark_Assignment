from src.Question_2.driver import (
    credit_card_df,
    original_partitions,
    df2,
    df3,
    df4
)


# Question 2.1
# Test whether the credit card DataFrame contains 5 records

def test_credit_card_data():

    assert credit_card_df.count() == 5


# Question 2.2
# Test whether the original number of partitions is greater than 0

def test_original_partitions():

    assert original_partitions > 0


# Question 2.3
# Test whether the number of partitions was increased to 5

def test_increased_partitions():

    assert df2.rdd.getNumPartitions() == 5


# Question 2.4
# Test whether the number of partitions was decreased back to the original number

def test_decreased_partitions():

    assert df3.rdd.getNumPartitions() == original_partitions


# Question 2.5
# Test whether the card number is masked except for the last 4 digits

def test_masked_card_number():

    result = df4.select(
        "masked_card_number"
    ).first()[0]

    assert result == "*************4567"