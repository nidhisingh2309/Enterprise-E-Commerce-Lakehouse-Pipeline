from pyspark.sql.functions import *

## Creating view for our analysis

fact_sales = spark.table("ecommerce.gold.fact_sales")
dim_customer = spark.table("ecommerce.gold.dim_customer")
dim_product = spark.table("ecommerce.gold.dim_product")
dim_seller = spark.table("ecommerce.gold.dim_seller")
dim_date = spark.table("ecommerce.gold.dim_date")

fact_sales.createOrReplaceTempView("fact_sales")
dim_customer.createOrReplaceTempView("dim_customer")
dim_product.createOrReplaceTempView("dim_product")
dim_seller.createOrReplaceTempView("dim_seller")
dim_date.createOrReplaceTempView("dim_date")


## KPI 1-- Total Revenue
fact_sales.select(
    round(sum("total_order_value"),2).alias("Total Revenue")
).show()


# KPI 2: Total Orders
fact_sales.select(
    countDistinct("order_id").alias("Total Orders")
).show()

## KPI 3: Total Customers
fact_sales.select(
    countDistinct("customer_id").alias("Total Customers")
).show()

##KPI 4: Average Order value
fact_sales.select(
    round(avg("total_order_value"),2).alias("Average Order Value")
).show()

##KPI 5: Monthly Revenue trend
monthly_revenue = (
    fact_sales
    .groupBy(
        year("purchase_date").alias("Year"),
        month("purchase_date").alias("Month")
    )
    .agg(
        round(sum("total_order_value"),2).alias("Revenue")
    )
    .orderBy("Year","Month")
)

display(monthly_revenue)

##KPI 6: Monthly Order Trend
monthly_orders = (
    fact_sales
    .groupBy(
        year("purchase_date").alias("Year"),
        month("purchase_date").alias("Month")
    )
    .agg(
        countDistinct("order_id").alias("Orders")
    )
    .orderBy("Year","Month")
)

display(monthly_orders)

## KPI 7: Revenue by Product Category
category_revenue = (
    fact_sales
    .join(dim_product,"product_id")
    .groupBy("product_category")
    .agg(
        round(sum("total_order_value"),2).alias("Revenue")
    )
    .orderBy(desc("Revenue"))
)

display(category_revenue)

## KPI 8: Top 10 products
top_products = (
    fact_sales
    .groupBy("product_id")
    .agg(
        round(sum("total_order_value"),2).alias("Revenue")
    )
    .orderBy(desc("Revenue"))
    .limit(10)
)

display(top_products)

## KPI 9:Top sellers
top_sellers = (
    fact_sales
    .groupBy("seller_id")
    .agg(
        round(sum("total_order_value"),2).alias("Revenue")
    )
    .orderBy(desc("Revenue"))
    .limit(10)
)

display(top_sellers)

## KPI 10:Revenue by Seller state
seller_state = (
    fact_sales
    .join(dim_seller,"seller_id")
    .groupBy("seller_state")
    .agg(
        round(sum("total_order_value"),2).alias("Revenue")
    )
    .orderBy(desc("Revenue"))
)

display(seller_state)

##KPI 11: Revenue by Customer state
customer_state = (
    fact_sales
    .join(dim_customer,"customer_id")
    .groupBy("customer_state")
    .agg(
        round(sum("total_order_value"),2).alias("Revenue")
    )
    .orderBy(desc("Revenue"))
)

display(customer_state)

##KPI 12:Payment Method Distribution
payment_distribution = (
    fact_sales
    .groupBy("payment_type")
    .count()
    .orderBy(desc("count"))
)

display(payment_distribution)

##KPI 13: Average Review by category
category_review = (
    fact_sales
    .join(dim_product,"product_id")
    .groupBy("product_category")
    .agg(
        round(avg("review_score"),2).alias("Average Review")
    )
    .orderBy(desc("Average Review"))
)

display(category_review)

##KPI 14: Delivery Performance
delivery = (
    fact_sales.select(
        round(avg("delivery_days"),2).alias("Average Delivery Days"),
        round(avg("delivery_delay_days"),2).alias("Average Delay")
    )
)

delivery.show()

##KPI 15: Customer Lifetime value
clv = (
    fact_sales
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("Orders"),
        round(sum("total_order_value"),2).alias("Lifetime Value")
    )
    .orderBy(desc("Lifetime Value"))
)

display(clv.limit(20))

### KPI 16: Top 5 Products per category
category_sales = (
    fact_sales
    .join(dim_product,"product_id")
    .groupBy("product_category","product_id")
    .agg(
        sum("total_order_value").alias("Revenue")
    )
)

window = Window.partitionBy("product_category").orderBy(desc("Revenue"))

top5 = (
    category_sales
    .withColumn("Rank",dense_rank().over(window))
    .filter(col("Rank")<=5)
)

display(top5)

###KPI 17: Monthly Revenue Growth
monthly = (
    fact_sales
    .groupBy(
        year("purchase_date").alias("Year"),
        month("purchase_date").alias("Month")
    )
    .agg(
        sum("total_order_value").alias("Revenue")
    )
)

window = Window.orderBy("Year","Month")

growth = (
    monthly
    .withColumn(
        "Previous Revenue",
        lag("Revenue").over(window)
    )
    .withColumn(
        "Growth",
        col("Revenue")-col("Previous Revenue")
    )
)

display(growth)

###KPI 18: Repeat Customers
repeat = (
    fact_sales
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("Orders")
    )
    .withColumn(
        "Customer Type",
        when(col("Orders")==1,"One-Time")
        .otherwise("Repeat")
    )
    .groupBy("Customer Type")
    .count()
)

display(repeat)

##KPI 19: Average Basket Size
basket = (
    fact_sales
    .groupBy("order_id")
    .agg(
        count("order_item_id").alias("Items")
    )
)

basket.select(
    round(avg("Items"),2).alias("Average Basket Size")
).show()

###KPI 20:Revenue contribution by category
total_revenue = fact_sales.agg(sum("total_order_value")).collect()[0][0]

contribution = (
    fact_sales
    .join(dim_product,"product_id")
    .groupBy("product_category")
    .agg(
        sum("total_order_value").alias("Revenue")
    )
    .withColumn(
        "Contribution %",
        round(col("Revenue")*100/total_revenue,2)
    )
    .orderBy(desc("Revenue"))
)

display(contribution)







