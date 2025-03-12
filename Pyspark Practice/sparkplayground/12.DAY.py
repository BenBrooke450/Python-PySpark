






def drop_duplicates(df: DataFrame) -> DataFrame:


    filtered_columns = [col for col in df.columns if not (col.startswith('abc') or col.startswith('abc'))]

    # Filtering out duplicates from the dataframe

    win = Window.partitionBy(filtered_columns).orderBy("abc")

    df = df.withColumn("drv_df_Rank", row_number().over(win))

    df = df.filter(col("drv_df_Rank") == 1).drop(col("drv_df_Rank"))

    return df
















Post_Void_Res_vol = parameters.get('Post_Void_Res_vol')

param_Inc_Exc_Code = parameters.get('param_Inc_Exc_Code')

# COMMAND ----------

if Post_Void_Res_vol == None:
    Post_Void_Res_vol = 350

if param_Inc_Exc_Code == None:
    param_Inc_Exc_Code = 'EX06'









df = df.withColumn('drv_Message',

                   when((col('URSTRESN') > Post_Void_Res_vol) &

                        #(col('IEORRES') == 'Y') &

                        (~col('drv_Incl/Excl_violated').contains(param_Inc_Exc_Code)),

                        lit("WARNING: Value within excluding protocol range and related excl criteria not entered"))

                   .otherwise(when((col('URSTRESN') > Post_Void_Res_vol) &

                                    (col('IEORRES') == 'Y') &

                                    (col('drv_Incl/Excl_violated').contains(param_Inc_Exc_Code)),

                                    lit("OK: Value within excluding protocol range and related excl criteria entered as 'not met'"))

                               .otherwise(when((col('URSTRESN') <= Post_Void_Res_vol) &

                                               #(col('IEORRES') == 'Y') &

                                                (~col('drv_Incl/Excl_violated').contains(param_Inc_Exc_Code)),

                                                lit("OK: Value not within excluding protocol range and the related excl criteria not entered"))

                                           .otherwise(when((col('URSTRESN') <= Post_Void_Res_vol) &

                                                            (col('IEORRES') == 'Y') &

                                                            (col('drv_Incl/Excl_violated').contains(param_Inc_Exc_Code)),

                                                            lit("WARNING: Value not within excluding protocol range and the related excl criteria entered as 'met'"))))))







