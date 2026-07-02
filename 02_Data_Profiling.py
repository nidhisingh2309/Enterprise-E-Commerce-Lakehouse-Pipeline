from pyspark.sql.functions import *

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


## see the basic profile

"""for name , df in tables.items():
    print(f"\n{name}")
    print("-"*50)
    print(f"Rows:{df.count()}")
    print(f"Columns:{len(df.columns)}")
    df.printSchema()
    display(df.limit(5))

## Check 2 -- Check for the missing values
for name,df in tables.items():
    print(f"\n{name}")
    print("-"*50)
    missing=df.select([
        count(when(col(c).isNull(),c)).alias(c)
        for c in df.columns
    ])
    display(missing)
"""


## Check 3 -- Duplicate check
customers.groupBy("customer_id").count().filter(col("count")>1).show()
orders.groupBy("order_id").count().filter(col("count")>1).show()
order_items.groupBy("order_item_id").count().filter(col("count")>1).show() ## has duplicates

## Check 4 -- Distinct counts
customers.select(countDistinct("customer_id")).show()
orders.select(countDistinct("order_id")).show()

## Check 5 -- DEscriptive stats
order_payments.describe().show()
products.describe().show()



## Observations so far
# Profiling Summary

"""### Customers
- No duplicate customer_id.
- No missing primary keys.

### Orders
- Order dates are stored as timestamps.
- No duplicate order_id.

### Reviews
- Review comments contain many null values.
- Review scores range from 1 to 5.

### Products
- Some product categories are missing.
- Product dimensions require validation."""


