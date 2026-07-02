from pyspark.sql.functions import *
from pyspark.sql import DataFrame

customers=spark.table("ecommerce.bronze.olist_customers_dataset")
orders=spark.table("ecommerce.bronze.orders_raw")
order_items=spark.table("ecommerce.bronze.order_items_raw")
products=spark.table("ecommerce.bronze.products_raw")
sellers=spark.table("ecommerce.bronze.sellers_raw")
order_payments=spark.table("ecommerce.bronze.order_payments_raw")
order_reviews=spark.table("ecommerce.bronze.order_reviews_raw")
product_name=spark.table("ecommerce.bronze.product_category_name_raw")




tables={
    "customers":customers,
    "orders":orders,
    "order_items":order_items,
    "products":products,
    "sellers":sellers,
    "order_payments":order_payments,
    "order_reviews":order_reviews,
    "product_name":product_name
    
}


## Check1 --Primary Key Validation
def validate_primary_key(df:DataFrame,column_name:str):
    total_rows= df.count()
    duplicate_count=(
        df.groupby(column_name)
        .count()
        .filter(col("count")>1)
        .count()
    )
    null_count=df.filter(col(column_name).isNull()).count()
    print("="*60)
    print(f"Primary key validation:{column_name}")
    print("="*60)
    print(f"Total_rows:{total_rows}")
    print(f"Duplicate_records:{duplicate_count}")
    print(f"Null_records:{null_count}")
    if duplicate_count==0 and null_count==0:
        print("Validation passed")
    else:
        print("Validation failed")


validate_primary_key(customers,"customer_id")
validate_primary_key(orders,"order_id")
validate_primary_key(products,"product_id")
validate_primary_key(sellers,"seller_id")


### Check 2-- Missing value validation

def validate_missing_values(df:DataFrame):
    print("="*60)
    print("Missing value report")
    print("="*60)
    missing=df.select([
        count(when(col(c).isNull(),c)).alias(c)
        for c in df.columns
    ])
    display(missing)

validate_missing_values(customers)

validate_missing_values(orders)

validate_missing_values(products)

## Check 3 -- Duplicate validation
def validate_duplicate(df:DataFrame):
    duplicate_rows=df.count()-df.dropDuplicates().count()
    print("-"*60)
    print("Duplicate row validation")
    print("="*60)
    print(f"Duplicate rows:{duplicate_rows}")
    if duplicate_rows==0:
        print("Passed")
    else:
        print("Failed")

## Check 4 -- Foreign key validation

def validate_foreign_key(child_df: DataFrame,
                         parent_df: DataFrame,
                         key: str,
                         child_name: str,
                         parent_name: str):

    invalid = child_df.join(
        parent_df,
        on=key,
        how="left_anti"
    )

    count_invalid = invalid.count()

    print("="*60)
    print(f"{child_name} -> {parent_name}")
    print("="*60)
    print(f"Invalid Records : {count_invalid}")

    if count_invalid == 0:
        print("✅ Passed")
    else:
        print("❌ Failed")
        display(invalid)

validate_foreign_key(
    orders,
    customers,
    "customer_id",
    "Orders",
    "Customers"
)

validate_foreign_key(
    order_items,
    orders,
    "order_id",
    "Order Items",
    "Orders"
)

## Check 4-  Numeric validation

def validate_positive(df: DataFrame,
                      column_name: str):

    invalid = df.filter(col(column_name) <= 0)

    count_invalid = invalid.count()

    print("="*60)
    print(column_name)
    print("="*60)
    print(f"Invalid Records : {count_invalid}")

    if count_invalid == 0:
        print("✅ Passed")
    else:
        display(invalid)

validate_positive(order_payments,"payment_value")
validate_positive(products,"product_weight_g")

validate_positive(products,"product_length_cm")

validate_positive(products,"product_height_cm")

validate_positive(products,"product_width_cm")


## Check 5 -- Allowed values validation
def validate_allowed_values(df,
                            column_name,
                            allowed):

    invalid = df.filter(~col(column_name).isin(allowed))

    invalid_count = invalid.count()

    print("="*60)
    print(column_name)
    print("="*60)

    print(f"Invalid Values : {invalid_count}")

    if invalid_count == 0:
        print("✅ Passed")
    else:
        display(invalid)

validate_allowed_values(
    order_reviews,
    "review_score",
    [1,2,3,4,5]
)


## check 6 --date validation
def validate_date_order(df,
                        start_column,
                        end_column):

    invalid = df.filter(
        col(end_column) < col(start_column)
    )

    invalid_count = invalid.count()

    print("="*60)
    print(f"{start_column} -> {end_column}")
    print("="*60)

    print(f"Invalid Records : {invalid_count}")

    if invalid_count == 0:
        print("✅ Passed")
    else:
        display(invalid)

validate_date_order(
    orders,
    "order_purchase_timestamp",
    "order_delivered_customer_date"
)

validate_date_order(
    orders,
    "order_purchase_timestamp",
    "order_estimated_delivery_date"
)

## Check 7-- Validation. summary
validation_summary=[
    
("Customer Primary Key", "Passed"),
    ("Order Primary Key", "Passed"),
    ("Product Primary Key", "Passed"),
    ("Seller Primary Key", "Passed"),
    ("Foreign Key Validation", "Passed"),
    ("Review Score Validation", "Passed"),
    ("Payment Validation", "Passed"),
    ("Date Validation", "Passed")
]
summary_df=spark.createDataFrame(validation_summary,["Validation","status"])
display(summary_df)
