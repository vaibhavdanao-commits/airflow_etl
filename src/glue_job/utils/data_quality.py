import pandas as pd


def calculate_data_quality(
    df,
    required_columns=None,
    unique_key=None,
    not_null_columns=None,
    accepted_values=None,
    numeric_ranges=None,
):
    # Your existing data quality validation logic goes here
    pass


def validate_orders_data(df):
    return calculate_data_quality(
        df=df,
        required_columns=[
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "order_status",
        ],
        unique_key="order_id",
        not_null_columns=[
            "customer_id",
            "product_id",
            "quantity",
        ],
        accepted_values={
            "order_status": ["CANCELLED", "SHIPPED", "DELIVERED"],
        },
        numeric_ranges={
            "quantity": (1, 10000),
            "unit_price": (0.01, 1000000),
        },
    )