from pyspark.sql.functions import *
from pyspark.sql.types import *

customers = spark.table("ecommerce.silver.customers")

orders = spark.table("ecommerce.silver.orders")

order_items = spark.table("ecommerce.silver.order_items")

payments = spark.table("ecommerce.silver.payments")

products = spark.table("ecommerce.silver.products")

reviews = spark.table("ecommerce.silver.reviews")

sellers = spark.table("ecommerce.silver.sellers")

category = spark.table("ecommerce.silver.product_name")

tables = {
    "customers": customers,
    "orders": orders,
    "order_items": order_items,
    "payments": payments,
    "products": products,
    "reviews": reviews,
    "sellers": sellers,
    "category": category,

}

for name, df in tables.items():
    print("=" * 60)
    print(f"Table : {name}")
    print(f"Rows  : {df.count()}")
    print(f"Columns : {len(df.columns)}")


### Step 1 - Creating the customer dimension table
dim_customer = (
    customers
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state"
    )
    .dropDuplicates()
)

## check row count
print(f"Rows: {dim_customer.count()}")

## check duplicate customer ids 
duplicate_customers = (
    dim_customer
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Customer IDs:", duplicate_customers.count())

## check for NULL primary keys
null_customers = dim_customer.filter(col("customer_id").isNull())

print("Null Customer IDs:", null_customers.count())
display(dim_customer.limit(10))

## saving the dim_customer_table
dim_customer.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("ecommerce.gold.dim_customer")

## validation
spark.table("ecommerce.gold.dim_customer").count()


## Step 2 -- Creating the product dim table
dim_product = (
    products.alias("p")
    .join(
        category.alias("c"),
        on="product_category_name",
        how="left"
    )
    .select(
        col("product_id"),
        col("product_category_name"),
        col("product_category_name_english"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm")
    )
    .dropDuplicates()
)

## Validate the dim 
print(f"Rows: {dim_product.count()}")

## Check for duplicate Product IDs
duplicate_products = (
    dim_product
    .groupBy("product_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Product IDs:", duplicate_products.count())

## Check for NULL Product IDs
null_products = (
    dim_product
    .filter(col("product_id").isNull())
)

print("Null Product IDs:", null_products.count())

## Check missing English Category names
missing_translation = (
    dim_product
    .filter(col("product_category_name_english").isNull())
)

print("Products without English category:", missing_translation.count())


display(dim_product.limit(10))

## Save the dim_product table
dim_product.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("ecommerce.gold.dim_product")

## Validate the table
spark.table("ecommerce.gold.dim_product").count()



from pyspark.sql.functions import col

(
    dim_product
    .filter(col("product_category_name_english").isNull())
    .groupBy("product_category_name")
    .count()
    .orderBy(col("count").desc())
    .show(50, truncate=False)
)

dim_product = spark.table("ecommerce.gold.dim_product")

from pyspark.sql.functions import col, when


dim_product = dim_product.withColumn(
    "product_category_name_english",
    when(
        col("product_category_name_english").isNull(),
        "Unknown"
    ).otherwise(col("product_category_name_english"))
)

dim_product.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("ecommerce.gold.dim_product")

### Step 3 --Creating the dim_seller Table
from pyspark.sql.functions import *
sellers = spark.table("ecommerce.silver.sellers")

dim_seller = (
    sellers
    .select(
        "seller_id",
        "seller_city",
        "seller_state"
    )
    .dropDuplicates()
)

print("Rows:", dim_seller.count())

## Duplicate seller Ids
duplicate_sellers = (
    dim_seller
    .groupBy("seller_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Seller IDs:", duplicate_sellers.count())

## NULL seller IDs
null_sellers = dim_seller.filter(
    col("seller_id").isNull()
)

print("Null Seller IDs:", null_sellers.count())
display(dim_seller.limit(10))

dim_seller.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("ecommerce.gold.dim_seller")

gold_seller = spark.table("ecommerce.gold.dim_seller")

print("Rows:", gold_seller.count())

display(gold_seller.limit(10))


orders=spark.table("ecommerce.silver.orders")
from pyspark.sql.functions import *
## Step 4-- Creating the dim_date table
date_range = orders.select(
    min("order_purchase_timestamp").alias("start_date"),
    max("order_purchase_timestamp").alias("end_date")
).collect()[0]

start_date = date_range["start_date"].date()
end_date = date_range["end_date"].date()

print(start_date)
print(end_date)

## Generating every date
from datetime import timedelta

dates = []

current_date = start_date

while current_date <= end_date:
    dates.append((current_date,))
    current_date += timedelta(days=1)

## Create spark data frame
dim_date = spark.createDataFrame(
    dates,
    ["date"]
)

from pyspark.sql.functions import (
    year,
    quarter,
    month,
    dayofmonth,
    weekofyear,
    date_format,
    when,
    dayofweek,
    concat_ws,
    lpad
)

dim_date = (
    dim_date
    .withColumn("year", year("date"))
    .withColumn("quarter", quarter("date"))
    .withColumn("month", month("date"))
    .withColumn("month_name", date_format("date", "MMMM"))
    .withColumn("week", weekofyear("date"))
    .withColumn("day", dayofmonth("date"))
    .withColumn("day_name", date_format("date", "EEEE"))
    .withColumn(
        "is_weekend",
        when(dayofweek("date").isin([1, 7]), "Yes").otherwise("No")
    )
    .withColumn(
        "year_month",
        concat_ws(
            "-",
            year("date"),
            lpad(month("date"), 2, "0")
        )
    )
)
print("Rows:", dim_date.count())
print(
    "Null Dates:",
    dim_date.filter(col("date").isNull()).count()
)
duplicates = (
    dim_date
    .groupBy("date")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Dates:", duplicates.count())
display(dim_date.limit(10))
dim_date.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("ecommerce.gold.dim_date")

gold_date = spark.table("ecommerce.gold.dim_date")

print(gold_date.count())

display(gold_date.limit(10))


## Final steppp-  Creating the fact table
from pyspark.sql.functions import *
from pyspark.sql.window import Window

orders = spark.table("ecommerce.silver.orders")

order_items = spark.table("ecommerce.silver.order_items")

payments = spark.table("ecommerce.silver.payments")

reviews = spark.table("ecommerce.silver.reviews")


print("Orders:", orders.count())

fact1 = orders.alias("o").join(
    order_items.alias("oi"),
    "order_id"
)

print(fact1.count())

print("Order Items:", order_items.count())

print("Payments:", payments.count())

print("Reviews:", reviews.count())

## Aggregate Payments
payments_agg = (
    payments
    .groupBy("order_id")
    .agg(
        sum("payment_value").alias("payment_value"),

        max("payment_installments").alias("payment_installments"),

        concat_ws(
            ", ",
            sort_array(
                collect_set("payment_type")
            )
        ).alias("payment_type")
    )
)

print("Original Payment Rows:", payments.count())

print("Aggregated Payment Rows:", payments_agg.count())

payments_agg.groupBy("order_id") \
.count() \
.filter(col("count") > 1) \
.show()

reviews_agg = (
    reviews
    .filter(col("order_id").isNotNull())
    .groupBy("order_id")
    .agg(
        avg("review_score").alias("review_score")
    )
)

print("Aggregated Reviews:", reviews_agg.count())

reviews_agg.groupBy("order_id") \
    .count() \
    .filter(col("count") > 1) \
    .show()

duplicates = (
    reviews_agg.groupBy("order_id")
    .count()
    .filter(col("count") > 1)
)

print(duplicates.count())
display(duplicates)

fact_sales = orders.alias("o")
fact_sales = (
    fact_sales.join(
        order_items.alias("oi"),
        col("o.order_id") == col("oi.order_id"),
        "inner"
    )
)

print("Rows after Orders + Order Items")

print(fact_sales.count())

fact_sales = (
    fact_sales.join(
        payments_agg.alias("p"),
        col("o.order_id") == col("p.order_id"),
        "left"
    )
)

print("Rows after Payment Join")

print(fact_sales.count())

fact_sales = (
    fact_sales.join(
        reviews.alias("r"),
        col("o.order_id") == col("r.order_id"),
        "left"
    )
)

print("Rows after Review Join")

print(fact_sales.count())

fact_sales = (
    fact_sales.select(

        # Order Information
        col("o.order_id"),

        col("oi.order_item_id"),

        col("o.customer_id"),

        to_date(
            col("o.order_purchase_timestamp")
        ).alias("purchase_date"),

        # Product & Seller
        col("oi.product_id"),

        col("oi.seller_id"),

        # Measures
        col("oi.price"),

        col("oi.freight_value"),

        col("p.payment_value"),

        col("p.payment_installments"),

        # Attributes
        col("p.payment_type"),

        col("r.review_score"),

        col("o.order_status"),

        col("o.order_purchase_timestamp"),

        col("o.order_delivered_customer_date"),

        col("o.order_estimated_delivery_date")
    )
)

fact_sales = (
    fact_sales

    .withColumn(
        "total_order_value",
        col("price") + col("freight_value")
    )

    .withColumn(
        "delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    )

    .withColumn(
        "delivery_delay_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date")
        )
    )
)

print("Fact Rows:", fact_sales.count())

duplicates = (
    fact_sales
    .groupBy(
        "order_id",
        "order_item_id"
    )
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Order Items:", duplicates.count())

print(
    "Null Customer IDs:",
    fact_sales.filter(
        col("customer_id").isNull()
    ).count()
)

print(
    "Null Product IDs:",
    fact_sales.filter(
        col("product_id").isNull()
    ).count()
)

print(
    "Null Seller IDs:",
    fact_sales.filter(
        col("seller_id").isNull()
    ).count()
)

print(
    "Null Purchase Dates:",
    fact_sales.filter(
        col("purchase_date").isNull()
    ).count()
)

print(
    "Negative Prices:",
    fact_sales.filter(
        col("price") < 0
    ).count()
)

print(
    "Negative Freight:",
    fact_sales.filter(
        col("freight_value") < 0
    ).count()
)

print(
    "Negative Payment:",
    fact_sales.filter(
        col("payment_value") < 0
    ).count()
)

fact_sales.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("ecommerce.gold.fact_sales")

gold_fact = spark.table("ecommerce.gold.fact_sales")

print("Rows:", gold_fact.count())

display(gold_fact.limit(10))

from pyspark.sql.functions import *

fact_sales=spark.table("ecommerce.gold.fact_sales")

fact_sales = fact_sales.dropDuplicates(["order_id", "order_item_id"])