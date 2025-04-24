
from pyspark.sql.functions import (col, lit, contains, when,
                                    count, row_number, concat, upper, rank, avg, round, sum, max)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType



df_electric_cars_casualty = (spark.read.format("delta")
                             .load(f"{gold_folder_path}/presentation_electric_cars_casualty_statistics_2023_data"))




df_electric_cars_casualty.withColumn("average_age_of_vehicle",avg(col("age_of_vehicle"))
                                     .over(Window.partitionBy("generic_make_model").orderBy("generic_make_model"))).show()

+-------------+-------------+--------------+------------------+--------------+--------------------+---------------------+-----------------------+---------------------+----------------------+
|sex_of_driver|age_of_driver|age_of_vehicle|generic_make_model|accident_index|      ingestion_date|electric_vehicle_type|generic_make_model_Rank|Average_age_over_type|average_age_of_vehicle|
+-------------+-------------+--------------+------------------+--------------+--------------------+---------------------+-----------------------+---------------------+----------------------+
|            1|           47|             4|       AUDI E-TRON| 2023010421558|2025-02-28 23:09:...|                    Y|                      1|                 38.0|                   2.0|
|            1|           35|             3|       AUDI E-TRON| 2023010422882|2025-02-28 23:09:...|                    Y|                      2|                 38.0|                   2.0|
|            1|           40|             3|       AUDI E-TRON| 2023010437244|2025-02-28 23:09:...|                    Y|                      3|                 38.0|                   2.0|
|            2|           50|             2|       AUDI E-TRON| 2023010439289|2025-02-28 23:09:...|                    Y|                      4|                 38.0|                   2.0|
|            1|           51|             1|       AUDI E-TRON| 2023010441627|2025-02-28 23:09:...|                    Y|                      5|                 38.0|                   2.0|
|            1|           44|             3|       AUDI E-TRON| 2023010442173|2025-02-28 23:09:...|                    Y|                      6|                 38.0|                   2.0|
|            2|           38|             2|       AUDI E-TRON| 2023010443196|2025-02-28 23:09:...|                    Y|                      7|                 38.0|                   2.0|
|            1|           41|             1|       AUDI E-TRON| 2023010451583|2025-02-28 23:09:...|                    Y|                     10|                 38.0|                   2.0|
|            2|           29|             0|       AUDI E-TRON| 2023041365143|2025-02-28 23:09:...|                    Y|                     13|                 38.0|                   2.0|
|            1|           53|             3|       AUDI E-TRON| 2023052300202|2025-02-28 23:09:...|                    Y|                     14|                 38.0|                   2.0|
|            1|           47|             1|       AUDI E-TRON| 2023052300577|2025-02-28 23:09:...|                    Y|                     15|                 38.0|                   2.0|
|            2|           36|             1|       AUDI E-TRON| 2023052300659|2025-02-28 23:09:...|                    Y|                     16|                 38.0|                   2.0|
|            1|           44|             2|       AUDI E-TRON| 2023052301622|2025-02-28 23:09:...|                    Y|                     17|                 38.0|                   2.0|
|            1|           55|             1|       AUDI E-TRON| 2023061387166|2025-02-28 23:09:...|                    Y|                     19|                 38.0|                   2.0|
|            1|           44|             4|       AUDI E-TRON| 2023351305432|2025-02-28 23:09:...|                    Y|                     20|                 38.0|                   2.0|
|            1|           71|             2|       AUDI E-TRON| 2023351340341|2025-02-28 23:09:...|                    Y|                     21|                 38.0|                   2.0|
|            1|           43|             1|       AUDI E-TRON| 2023351375703|2025-02-28 23:09:...|                    Y|                     22|                 38.0|                   2.0|
|            1|           21|             4|       AUDI E-TRON| 2023411270411|2025-02-28 23:09:...|                    Y|                     23|                 38.0|                   2.0|
|            2|           34|             1|       AUDI E-TRON| 2023411345432|2025-02-28 23:09:...|                    Y|                     24|                 38.0|                   2.0|
|            1|           27|             1|       AUDI E-TRON| 2023411394282|2025-02-28 23:09:...|                    Y|                     25|                 38.0|                   2.0|
+-------------+-------------+--------------+------------------+--------------+--------------------+---------------------+-----------------------+---------------------+----------------------+
only showing top 20 rows














(df_electric_cars_casualty.withColumn("rank_over_avg_age",row_number()
                                     .over(Window.partitionBy("generic_make_model")
                                           .orderBy("average_age_of_vehicle"))).filter(col("rank_over_avg_age")<3)
                                                    .orderBy(col("average_age_of_vehicle").desc()).show())

+-------------+-------------+--------------+------------------+--------------+--------------------+---------------------+-----------------------+---------------------+----------------------+-----------------+
|sex_of_driver|age_of_driver|age_of_vehicle|generic_make_model|accident_index|      ingestion_date|electric_vehicle_type|generic_make_model_Rank|Average_age_over_type|average_age_of_vehicle|rank_over_avg_age|
+-------------+-------------+--------------+------------------+--------------+--------------------+---------------------+-----------------------+---------------------+----------------------+-----------------+
|            1|           58|             4|        HONDA CR-V| 2023010419270|2025-02-28 23:09:...|                    Y|                      1|                 47.0|    11.543668122270743|                1|
|            2|           31|            19|        HONDA CR-V| 2023010420439|2025-02-28 23:09:...|                    Y|                      2|                 47.0|    11.543668122270743|                2|
|            2|           59|            12|   CHEVROLET SPARK| 2023010425478|2025-02-28 23:09:...|                    Y|                      1|                 41.0|                 11.05|                1|
|            3|           19|            10|   CHEVROLET SPARK| 2023010456894|2025-02-28 23:09:...|                    Y|                      3|                 41.0|                 11.05|                2|
|            2|           29|             5|      SMART FORTWO| 2023010420145|2025-02-28 23:09:...|                    Y|                      1|                 39.0|     10.79503105590062|                1|
|            2|           31|             6|      SMART FORTWO| 2023010420443|2025-02-28 23:09:...|                    Y|                      2|                 39.0|     10.79503105590062|                2|
|            1|           52|             5|       TOYOTA RAV4| 2023010419481|2025-02-28 23:09:...|                    Y|                      1|                 45.0|    10.014084507042254|                1|
|            1|           44|            18|       TOYOTA RAV4| 2023010422027|2025-02-28 23:09:...|                    Y|                      3|                 45.0|    10.014084507042254|                2|
|            2|           40|             8|          KIA SOUL| 2023010426140|2025-02-28 23:09:...|                    Y|                      2|                 50.0|     9.442307692307692|                1|
|            3|           54|             5|          KIA SOUL| 2023010461840|2025-02-28 23:09:...|                    Y|                      3|                 50.0|     9.442307692307692|                2|
|            1|           40|             7|     TESLA MODEL S| 2023010422146|2025-02-28 23:09:...|                    Y|                      2|                 43.0|     6.216216216216216|                1|
|            1|           35|             4|     TESLA MODEL S| 2023010427441|2025-02-28 23:09:...|                    Y|                      3|                 43.0|     6.216216216216216|                2|
|            1|           60|             2|       FORD RANGER| 2023010441489|2025-02-28 23:09:...|                    Y|                      1|                 43.0|     5.078947368421052|                1|
|            1|           42|             4|       FORD RANGER| 2023010443109|2025-02-28 23:09:...|                    Y|                      2|                 43.0|     5.078947368421052|                2|
|            2|           66|             4|     TESLA MODEL X| 2023010424377|2025-02-28 23:09:...|                    Y|                      1|                 38.0|                   4.8|                1|
|            2|           48|             6|     TESLA MODEL X| 2023010424690|2025-02-28 23:09:...|                    Y|                      2|                 38.0|                   4.8|                2|
|            2|           47|             4|     PORSCHE MACAN| 2023010422868|2025-02-28 23:09:...|                    Y|                      1|                 38.0|     4.449275362318841|                1|
|            3|           52|             4|     PORSCHE MACAN| 2023010428448|2025-02-28 23:09:...|                    Y|                      2|                 38.0|     4.449275362318841|                2|
|            1|           34|             5|            BMW I3| 2023010420912|2025-02-28 23:09:...|                    Y|                      1|                 38.0|    4.3578947368421055|                1|
|            1|           29|             5|            BMW I3| 2023010421806|2025-02-28 23:09:...|                    Y|                      4|                 38.0|    4.3578947368421055|                2|
+-------------+-------------+--------------+------------------+--------------+--------------------+---------------------+-----------------------+---------------------+----------------------+-----------------+
only showing top 20 rows











test = (df_electric_cars_casualty.withColumn("rank_over_avg_age",row_number()
                                            .over(Window.partitionBy("generic_make_model")
                                                  .orderBy("average_age_of_vehicle")))
        .filter(col("rank_over_avg_age")<3).orderBy(col("average_age_of_vehicle").desc()))

test = test.select("generic_make_model","average_age_of_vehicle","rank_over_avg_age")

test.groupBy("generic_make_model")
    .pivot("rank_over_avg_age")
    .max("average_age_of_vehicle").display()
+-------------------+------------------+------------------+
| generic_make_model|                 1|                 2|
+-------------------+------------------+------------------+
|        AUDI E-TRON|               2.0|               2.0|
|            AUDI Q4|0.8108108108108109|0.8108108108108109|
|            AUDI Q8|2.1538461538461537|2.1538461538461537|
|             BMW I3|4.3578947368421055|4.3578947368421055|
|             BMW I4|0.5588235294117647|0.5588235294117647|
|             BMW IX|0.5172413793103449|0.5172413793103449|
|    CHEVROLET SPARK|             11.05|             11.05|
|FORD MUSTANG MACH-E|1.0714285714285714|1.0714285714285714|
|        FORD RANGER| 5.078947368421052| 5.078947368421052|
|         HONDA CR-V|11.543668122270743|11.543668122270743|
|      HYUNDAI IONIQ| 3.694704049844237| 3.694704049844237|
|       HYUNDAI KONA| 1.948356807511737| 1.948356807511737|
|      JAGUAR I-PACE|2.5185185185185186|2.5185185185185186|
|            KIA EV6|              0.85|              0.85|
|           KIA NIRO|2.4273318872017353|2.4273318872017353|
|           KIA SOUL| 9.442307692307692| 9.442307692307692|
|       NISSAN ARIYA|               0.3|               0.3|
|        NISSAN LEAF|3.6700507614213196|3.6700507614213196|
|      PORSCHE MACAN| 4.449275362318841| 4.449275362318841|
|     PORSCHE TAYCAN|1.2857142857142858|1.2857142857142858|
+-------------------+------------------+------------------+
only showing top 20 rows





####################################################



df_rs_fa = df_rs_fa.withColumn(
    'drv_greater_5_prc_bm_blasts',
    when(
        col('FATESTCD') == 'OCCUR') & (col('FAOBJ') == ‘ABC’) & (col('FAORRES') == 'Y'),
            first(col('FAORRES')).over(win) == 1, 'Y').otherwise('N')
            )


"""
This won't work as it will take the first of within the over statement NOT (col('FAORRES') == 'Y'
"""




####################################################





win = Window.partitionBy('SUBJID', 'VISIT').orderBy('SUBJID')

df_rs_fa = df_rs_fa.withColumn(
    'drv_greater_5_prc_bm_blasts',
    when(
        max(when(
            (col('FATESTCD') == 'OCCUR') & (col('FAOBJ') == ‘ABC’) & (col('FAORRES') == 'Y'),lit(1)).otherwise(0))
            .over(win) == 1, 'Y').otherwise('N')
            )

df_rs_fa = df_rs_fa.withColumn(
    'drv_reapp_blasts_blood',
    when(
        max(when(
            (col('FATESTCD') == 'OCCUR') & (col('FAOBJ') == ‘ABC’) & (col('FAORRES') == 'Y'),lit(1)).otherwise(0))
            .over(win) == 1, 'Y').otherwise('N')
            )

df_rs_fa = df_rs_fa.withColumn(
    'drv_dev_emd',
    when(
        max(when(
            (col('FATESTCD') == 'OCCUR') & (col('FAOBJ') == ‘ABC’) & (col('FAORRES') == 'Y'),lit(1)).otherwise(0))
            .over(win) == 1, 'Y').otherwise('N')
            )

df_rs_fa = df_rs_fa.withColumn(
    'drv_molecular_relapse',
    when(
        max(when(
            (col('FATESTCD') == 'OCCUR') & (col('FAOBJ') == ‘ABC’) & (col('FAORRES') == 'Y'),lit(1)).otherwise(0))
            .over(win) == 1, 'Y').otherwise('N')
            )



