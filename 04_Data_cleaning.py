from pyspark.sql.functions import *
from pyspark.sql.types import *

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


## Step 1- Removing duplicates
def remove_duplicates(df):

    before = df.count()

    df = df.dropDuplicates()

    after = df.count()

    print(f"Duplicates Removed : {before-after}")

    return df

## Step 2- Trim string columns
def trim_strings(df):

    for column, dtype in df.dtypes:

        if dtype == "string":

            df = df.withColumn(
                column,
                trim(col(column))
            )

    return df

## Step 3-Convert empty strings to NUll
def empty_to_null(df):

    for column, dtype in df.dtypes:

        if dtype == "string":

            df = df.withColumn(
                column,
                when(trim(col(column))=="",None)
                .otherwise(col(column))
            )

    return df

    ##Step 4 -Standarize the column names 
def standardize_columns(df):

        for c in df.columns:

            df = df.withColumnRenamed(
            c,
            c.lower().replace(" ","_")
        )
            return df


## Cleaning the customers
customers_clean = customers

customers_clean = remove_duplicates(customers_clean)

customers_clean = trim_strings(customers_clean)

customers_clean = empty_to_null(customers_clean)

customers_clean = standardize_columns(customers_clean)

## Cleaning the orders
orders.printSchema()
orders_clean = orders.withColumn(
    "order_purchase_timestamp",
    to_timestamp("order_purchase_timestamp")
)

## Cleaning the products
products_clean = products.fillna({
    "product_category_name":"Unknown"
})

numeric_columns = [
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
]

products_clean = products_clean.fillna(0, subset=numeric_columns)


## Cleaning the reviews
reviews_clean = order_reviews.fillna({

    "review_comment_title":"No Review",

    "review_comment_message":"No Review"

})

## Cleaning the payments
payments_clean = order_payments.filter(
    col("payment_value")>0
)

## Cleaning the sellers
sellers_clean = remove_duplicates(sellers)

sellers_clean = trim_strings(sellers_clean)

## Cleaning mode check
summary = [

("Customers",
customers.count(),
customers_clean.count()),

("Orders",
orders.count(),
orders_clean.count()),

("Products",
products.count(),
products_clean.count()),

("Payments",
order_payments.count(),
payments_clean.count()),

("Reviews",
order_reviews.count(),
reviews_clean.count())

]

summary_df = spark.createDataFrame(
summary,
["Table","Before","After"]
)

display(summary_df)


### Saving these to Silver schema
customers_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("ecommerce.silver.customers")

orders_clean.write.mode("overwrite").format("delta").saveAsTable("ecommerce.silver.orders")
products_clean.write.mode("overwrite").format("delta").saveAsTable("ecommerce.silver.products")
payments_clean.write.mode("overwrite").format("delta").saveAsTable("ecommerce.silver.payments")
reviews_clean.write.mode("overwrite").format("delta").saveAsTable("ecommerce.silver.reviews")
sellers_clean.write.mode("overwrite").format("delta").saveAsTable("ecommerce.silver.sellers")

product_name_clean=product_name
product_name_clean.write.mode("overwrite").format("delta").saveAsTable("ecommerce.silver.product_name")

order_items_clean=order_items
order_items_clean.write.mode("overwrite").format("delta").saveAsTable("ecommerce.silver.order_items")

