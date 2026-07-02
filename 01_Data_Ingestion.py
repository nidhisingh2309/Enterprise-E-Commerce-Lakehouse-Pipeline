from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("Show tables In ecommerce.bronze").show(truncate=False)

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

for name, df in tables.items():

    print("="*60)

    print(f"TABLE : {name}")

    print("="*60)

    print(f"Rows : {df.count()}")

    print(f"Columns : {len(df.columns)}")

    df.printSchema()

    display(df.limit(5))
##### Now generate a summary of whatever is present
summary=[]
for name,df in tables.items():
    summary.append((
        name,
        df.count(),
        len(df.columns)
    ))

summary_df=spark.createDataFrame(
    summary,
    ["Table name","Rows","Columns"]

)
display(summary_df)