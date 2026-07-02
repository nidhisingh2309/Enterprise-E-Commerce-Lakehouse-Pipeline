# ============================================================
# Customer Segmentation using K-Means Clustering
#
# Purpose:
# Segment customers into different groups based on purchasing
# behaviour using engineered customer features.
#
# Input:
# customer_features (Feature Engineering Layer)
#
# Output:
# customer_segments
# ============================================================

from pyspark.sql.functions import *

from pyspark.ml.feature import VectorAssembler

from pyspark.ml.feature import StandardScaler

from pyspark.ml.clustering import KMeans

from pyspark.ml.evaluation import ClusteringEvaluator

## Load customer features
customer_features = spark.table(
    "`ecommerce`.feature_store.customer_features"
)

display(customer_features)

## select features
features = [

    "total_orders",

    "total_spend",

    "average_order_value",

    "average_review_score",

    "average_delivery_days",

    "recency_days",

    "purchase_frequency"

]

## Check missing values
customer_features.select([
    count(
        when(col(c).isNull(), c)
    ).alias(c)
    for c in features
]).show()

## Handle missing values
customer_features = customer_features.fillna(0)

## Assemble Feature vector
assembler = VectorAssembler(

    inputCols=features,

    outputCol="features"

)

assembled = assembler.transform(customer_features)

## Standardize features
scaler = StandardScaler(

    inputCol="features",

    outputCol="scaled_features",

    withMean=True,

    withStd=True

)

scaler_model = scaler.fit(assembled)

scaled_data = scaler_model.transform(assembled)

## Train K-means models
kmeans = KMeans(

    featuresCol="scaled_features",

    predictionCol="cluster",

    k=4,

    seed=42

)

model = kmeans.fit(scaled_data)

## Predict clusters
customer_segments = model.transform(scaled_data)

display(customer_segments)

## Cluster evaluation
evaluator = ClusteringEvaluator(

    featuresCol="scaled_features",

    predictionCol="cluster"

)

silhouette = evaluator.evaluate(customer_segments)

print("Silhouette Score :", silhouette)

## Cluster distribution
display(

    customer_segments

    .groupBy("cluster")

    .count()

    .orderBy("cluster")

)

##Cluster profiles
cluster_summary = (

    customer_segments

    .groupBy("cluster")

    .agg(

        round(avg("total_orders"),2).alias("Avg Orders"),

        round(avg("total_spend"),2).alias("Avg Spend"),

        round(avg("average_order_value"),2).alias("Avg Order Value"),

        round(avg("average_review_score"),2).alias("Avg Review"),

        round(avg("average_delivery_days"),2).alias("Avg Delivery"),

        round(avg("recency_days"),2).alias("Avg Recency")

    )

)

display(cluster_summary)

## Busineess ineterpretation
customer_segments = customer_segments.withColumn(

    "segment",

    when(col("cluster")==0,"Premium")

    .when(col("cluster")==1,"New")

    .when(col("cluster")==2,"Inactive")

    .otherwise("Regular")

)

## Segment distribution
display(

customer_segments

.groupBy("segment")

.count()

)

## Save results
customer_segments.write.mode("overwrite").saveAsTable(

"ecommerce.feature_store.customer_segments"

)

## ValidTION
spark.sql("""

SELECT *

FROM ecommerce.feature_store.customer_segments

LIMIT 10

""").show()

##Summary
print("="*60)

print("Customer Segmentation Completed Successfully")

print("="*60)

print("✔ Customer Features Loaded")

print("✔ Feature Vector Created")

print("✔ Features Standardized")

print("✔ K-Means Model Trained")

print("✔ Customers Segmented")

print("✔ Results Saved")



