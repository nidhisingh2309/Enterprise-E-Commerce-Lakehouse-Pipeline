from pyspark.sql.functions import *

fact_sales = spark.table("ecommerce.gold.fact_sales")

dim_customer = spark.table("ecommerce.gold.dim_customer")

dim_product = spark.table("ecommerce.gold.dim_product")

dim_seller = spark.table("ecommerce.gold.dim_seller")

dim_date = spark.table("ecommerce.gold.dim_date")

## Step 1 - Row count validation
print("="*60)
print("ROW COUNT VALIDATION")
print("="*60)

print(f"Fact Sales      : {fact_sales.count():,}")
print(f"Dim Customer    : {dim_customer.count():,}")
print(f"Dim Product     : {dim_product.count():,}")
print(f"Dim Seller      : {dim_seller.count():,}")
print(f"Dim Date        : {dim_date.count():,}")

## Step 2 - NULL primary key validation
print("="*60)
print("NULL PRIMARY KEY VALIDATION")
print("="*60)

print("Customer IDs :", dim_customer.filter(col("customer_id").isNull()).count())

print("Product IDs  :", dim_product.filter(col("product_id").isNull()).count())

print("Seller IDs   :", dim_seller.filter(col("seller_id").isNull()).count())

print("Dates        :", dim_date.filter(col("date").isNull()).count())

print("Order IDs    :", fact_sales.filter(col("order_id").isNull()).count())


##Step 3 -Duplicate primary key validation
customer_duplicates = (
    dim_customer
    .groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Customers :", customer_duplicates.count())

product_duplicates = (
    dim_product
    .groupBy("product_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Products :", product_duplicates.count())

seller_duplicates = (
    dim_seller
    .groupBy("seller_id")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Sellers :", seller_duplicates.count())


date_duplicates = (
    dim_date
    .groupBy("date")
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Dates :", date_duplicates.count())



###Step 4 --Fact table Validation
duplicates = (
    fact_sales
    .groupBy(
        "order_id",
        "order_item_id"
    )
    .count()
    .filter(col("count") > 1)
)

print("Duplicate Order Items :", duplicates.count())


##Step5 -- Integrity checks
invalid_customer = (
    fact_sales.join(
        dim_customer,
        "customer_id",
        "left_anti"
    )
)

print("Invalid Customer Keys :", invalid_customer.count())

invalid_product = (
    fact_sales.join(
        dim_product,
        "product_id",
        "left_anti"
    )
)

print("Invalid Product Keys :", invalid_product.count())

invalid_seller = (
    fact_sales.join(
        dim_seller,
        "seller_id",
        "left_anti"
    )
)

print("Invalid Seller Keys :", invalid_seller.count())

invalid_date = (
    fact_sales.join(
        dim_date,
        fact_sales.purchase_date == dim_date.date,
        "left_anti"
    )
)

print("Invalid Dates :", invalid_date.count())

##Step 6  Business rule validations
print(
    "Negative Prices :",
    fact_sales.filter(col("price") < 0).count()
)
print(
    "Negative Freight :",
    fact_sales.filter(col("freight_value") < 0).count()
)

print(
    "Negative Payment :",
    fact_sales.filter(col("payment_value") < 0).count()
)

print(
    "Invalid Review Scores :",
    fact_sales.filter(
        (col("review_score") < 1) |
        (col("review_score") > 5)
    ).count()
)

fact_sales.select(
    avg("price").alias("Average Price"),
    avg("payment_value").alias("Average Payment"),
    avg("review_score").alias("Average Review"),
    avg("delivery_days").alias("Average Delivery Days")
).show()



print("="*60)
print("GOLD LAYER VALIDATION COMPLETED")
print("="*60)

print("✔ Row Count Validation")
print("✔ Null Primary Key Validation")
print("✔ Duplicate Key Validation")
print("✔ Referential Integrity Validation")
print("✔ Business Rule Validation")
print("✔ Summary Statistics Validation")
