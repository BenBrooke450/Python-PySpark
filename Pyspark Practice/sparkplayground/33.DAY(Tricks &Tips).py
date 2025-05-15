

#1.Use .isIn() instead of multiple ORs

df.filter(df.country.isin("UK", "US", "IN"))



#2.Use Window Functions With Partition Pruning

window_spec = Window.partitionBy("user_id").orderBy("timestamp")
df.withColumn("rank", row_number().over(window_spec)).filter("rank = 1")


