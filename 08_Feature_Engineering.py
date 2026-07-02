## Loading the Gold tables
from pyspark.sql.functions import *
from pyspark.sql.window import Window

fact_sales = spark.table("`ecommerce`.gold.fact_sales")

dim_customer = spark.table("`ecommerce`.gold.dim_customer")

dim_product = spark.table("`ecommerce`.gold.dim_product")

dim_seller = spark.table("`ecommerce`.gold.dim_seller")

dim_date = spark.table("`ecommerce`.gold.dim_date")

##Customer feature engineering
customer_features = (
    fact_sales
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),

        round(sum("total_order_value"),2).alias("total_spend"),

        round(avg("total_order_value"),2).alias("average_order_value"),

        round(avg("review_score"),2).alias("average_review_score"),

        round(avg("delivery_days"),2).alias("average_delivery_days"),

        max("purchase_date").alias("last_purchase_date"),

        min("purchase_date").alias("first_purchase_date")
    )
)

display(customer_features)

## Customer recency
latest_date = fact_sales.select(max("purchase_date")).collect()[0][0]

customer_features = customer_features.withColumn(
    "recency_days",
    datediff(lit(latest_date), col("last_purchase_date"))
)

## Customer tenure
customer_features = customer_features.withColumn(
    "customer_tenure_days",
    datediff(
        col("last_purchase_date"),
        col("first_purchase_date")
    )
)

## Purchase frequency
customer_features = customer_features.withColumn(
    "purchase_frequency",
    round(
        col("total_orders") /
        (col("customer_tenure_days") + 1),
        2
    )
)
## custonmer segement
customer_features = customer_features.withColumn(

    "customer_segment",

    when(col("total_spend") >= 1000, "Premium")

    .when(col("total_spend") >= 500, "Gold")

    .otherwise("Regular")

)

##Section 3: Product Feature Engineering

product_features = (

    fact_sales

    .groupBy("product_id")

    .agg(

        count("*").alias("times_sold"),

        round(sum("total_order_value"),2).alias("revenue"),

        round(avg("price"),2).alias("average_price"),

        round(avg("review_score"),2).alias("average_rating"),

        round(avg("delivery_days"),2).alias("average_delivery_days")

    )

)

## Seller Feature engineering
seller_features = (

    fact_sales

    .groupBy("seller_id")

    .agg(

        countDistinct("order_id").alias("orders"),

        round(sum("total_order_value"),2).alias("revenue"),

        round(avg("review_score"),2).alias("average_rating"),

        round(avg("delivery_days"),2).alias("average_delivery_days"),

        round(avg("delivery_delay_days"),2).alias("average_delay")

    )

)

## Seller tier
seller_features = seller_features.withColumn(

    "seller_tier",

    when(col("revenue") >= 50000, "Platinum")

    .when(col("revenue") >= 20000, "Gold")

    .otherwise("Silver")

)

## Time based features
time_features = (

    fact_sales

    .withColumn(

        "purchase_year",

        year("purchase_date")

    )

    .withColumn(

        "purchase_month",

        month("purchase_date")

    )

    .withColumn(

        "purchase_quarter",

        quarter("purchase_date")

    )

    .withColumn(

        "purchase_day",

        dayofmonth("purchase_date")

    )

    .withColumn(

        "purchase_week",

        weekofyear("purchase_date")

    )

    .withColumn(

        "purchase_day_name",

        date_format("purchase_date","EEEE")

    )

)

## Saved engineered features
customer_features.write.mode("overwrite").saveAsTable(
    "`ecommerce`.feature_store.customer_features"
)

product_features.write.mode("overwrite").saveAsTable(
    "`ecommerce`.feature_store.product_features"
)

seller_features.write.mode("overwrite").saveAsTable(
    "`ecommerce`.feature_store.seller_features"
)

