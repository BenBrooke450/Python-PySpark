

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank, isnull, when, lit, collect_list
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

def process_sales_data(df):
    """
    Process sales data:
    - Filter out null prices
    - Rank products per customer by quantity
    - Mark top-ranked purchases
    - Collect product names per customer
    - Flip a boolean flag using ~
    """
    # 1. Filter out null prices
    df_clean = df.filter(~isnull(col("price")))  # Using the swiggly line here!

    # 2. Define window for ranking
    window_spec = Window.partitionBy("customer_id").orderBy(col("quantity").desc())

    # 3. Add rank column
    df_ranked = df_clean.withColumn("rank", rank().over(window_spec))

    # 4. Flag top purchase per customer
    df_flagged = df_ranked.withColumn("is_top_purchase", col("rank") == 1)

    # 5. Flip it using swiggly line ~
    df_flagged = df_flagged.withColumn("is_not_top", ~col("is_top_purchase"))

    # 6. Create summary list per customer using collect_list and list comprehension
    df_summary = df_flagged.groupBy("customer_id").agg(
        collect_list("product_name").alias("product_list")
    )

    return df_flagged, df_summary














from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, coalesce, lit, length, lower, regexp_replace,
    dense_rank, ntile, when, current_date, to_date, date_format,
    explode, countDistinct, avg, array_contains
)
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

def analyze_reviews(df):
    """
    Cleans and analyzes product reviews:
    - Cleans review text
    - Handles missing ratings
    - Ranks reviews by length
    - Buckets reviews into quartiles
    - Explodes tags
    - Aggregates category insights
    """

    # 1. Clean text fields: lowercase, remove punctuation
    df_clean = df.withColumn("review_text_clean", regexp_replace(lower(col("review_text")), "[^a-z\\s]", ""))

    # 2. Handle missing ratings using coalesce
    df_clean = df_clean.withColumn("rating_filled", coalesce(col("rating"), lit(3.0)))

    # 3. Convert date string to date format and calculate week
    df_clean = df_clean.withColumn("review_date", to_date(col("date"), "yyyy-MM-dd")) \
                       .withColumn("week_of_review", date_format(col("review_date"), "YYYY-ww"))

    # 4. Ranking and bucketing
    win_spec = Window.partitionBy("product_id").orderBy(length("review_text_clean").desc())

    df_ranked = df_clean.withColumn("dense_rank", dense_rank().over(win_spec)) \
                        .withColumn("quartile", ntile(4).over(win_spec))

    # 5. Explode tags (array of category labels)
    df_exploded = df_ranked.withColumn("tag", explode("categories"))

    # 6. Aggregate: avg rating per tag, count of reviews
    tag_summary = df_exploded.groupBy("tag").agg(
        countDistinct("review_id").alias("num_reviews"),
        avg("rating_filled").alias("avg_rating")
    ).orderBy(col("num_reviews").desc())

    return df_exploded, tag_summary



















