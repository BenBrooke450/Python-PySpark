

models_mak.withColumn("Rank",rank().over(Window.partitionBy().orderBy("generic_make_model"))) \
        .withColumn("Dense_Rank",dense_rank().over(Window.partitionBy().orderBy("generic_make_model"))) \
        .withColumn("row_over_cat",row_number().over(Window.partitionBy("generic_make_model").orderBy("generic_make_model"))) \
        .withColumn("row_over_all",row_number().over(Window.partitionBy().orderBy("generic_make_model"))).show(100)


+------------------+----+----------+------------+------------+
|generic_make_model|Rank|Dense_Rank|row_over_cat|row_over_all|
+------------------+----+----------+------------+------------+
|       AUDI E-TRON|   1|         1|           1|           1|
|       AUDI E-TRON|   1|         1|           2|           2|
|       AUDI E-TRON|   1|         1|           3|           3|
|       AUDI E-TRON|   1|         1|           4|           4|
|       AUDI E-TRON|   1|         1|           5|           5|
|       AUDI E-TRON|   1|         1|           6|           6|
|       AUDI E-TRON|   1|         1|           7|           7|
|       AUDI E-TRON|   1|         1|           8|           8|
|       AUDI E-TRON|   1|         1|           9|           9|
|       AUDI E-TRON|   1|         1|          10|          10|
|       AUDI E-TRON|   1|         1|          11|          11|
|       AUDI E-TRON|   1|         1|          12|          12|
|       AUDI E-TRON|   1|         1|          13|          13|
|       AUDI E-TRON|   1|         1|          14|          14|
|       AUDI E-TRON|   1|         1|          15|          15|
|       AUDI E-TRON|   1|         1|          16|          16|
|       AUDI E-TRON|   1|         1|          17|          17|
|       AUDI E-TRON|   1|         1|          18|          18|
|       AUDI E-TRON|   1|         1|          19|          19|
|       AUDI E-TRON|   1|         1|          20|          20|
|       AUDI E-TRON|   1|         1|          21|          21|
|       AUDI E-TRON|   1|         1|          22|          22|
|       AUDI E-TRON|   1|         1|          23|          23|
|       AUDI E-TRON|   1|         1|          24|          24|
|       AUDI E-TRON|   1|         1|          25|          25|
|       AUDI E-TRON|   1|         1|          26|          26|
|       AUDI E-TRON|   1|         1|          27|          27|
|       AUDI E-TRON|   1|         1|          28|          28|
|       AUDI E-TRON|   1|         1|          29|          29|
|       AUDI E-TRON|   1|         1|          30|          30|
|       AUDI E-TRON|   1|         1|          31|          31|
|       AUDI E-TRON|   1|         1|          32|          32|
|       AUDI E-TRON|   1|         1|          33|          33|
|       AUDI E-TRON|   1|         1|          34|          34|
|       AUDI E-TRON|   1|         1|          35|          35|
|       AUDI E-TRON|   1|         1|          36|          36|
|       AUDI E-TRON|   1|         1|          37|          37|
|       AUDI E-TRON|   1|         1|          38|          38|
|       AUDI E-TRON|   1|         1|          39|          39|
|       AUDI E-TRON|   1|         1|          40|          40|
|       AUDI E-TRON|   1|         1|          41|          41|
|       AUDI E-TRON|   1|         1|          42|          42|
|       AUDI E-TRON|   1|         1|          43|          43|
|       AUDI E-TRON|   1|         1|          44|          44|
|       AUDI E-TRON|   1|         1|          45|          45|
|       AUDI E-TRON|   1|         1|          46|          46|
|       AUDI E-TRON|   1|         1|          47|          47|
|       AUDI E-TRON|   1|         1|          48|          48|
|       AUDI E-TRON|   1|         1|          49|          49|
|       AUDI E-TRON|   1|         1|          50|          50|
|       AUDI E-TRON|   1|         1|          51|          51|
|       AUDI E-TRON|   1|         1|          52|          52|
|       AUDI E-TRON|   1|         1|          53|          53|
|       AUDI E-TRON|   1|         1|          54|          54|
|       AUDI E-TRON|   1|         1|          55|          55|
|       AUDI E-TRON|   1|         1|          56|          56|
|       AUDI E-TRON|   1|         1|          57|          57|
|           AUDI Q4|  58|         2|           1|          58|
|           AUDI Q4|  58|         2|           2|          59|
|           AUDI Q4|  58|         2|           3|          60|
|           AUDI Q4|  58|         2|           4|          61|
|           AUDI Q4|  58|         2|           5|          62|
|           AUDI Q4|  58|         2|           6|          63|
|           AUDI Q4|  58|         2|           7|          64|
|           AUDI Q4|  58|         2|           8|          65|
|           AUDI Q4|  58|         2|           9|          66|
|           AUDI Q4|  58|         2|          10|          67|
|           AUDI Q4|  58|         2|          11|          68|
|           AUDI Q4|  58|         2|          12|          69|
|           AUDI Q4|  58|         2|          13|          70|
|           AUDI Q4|  58|         2|          14|          71|
|           AUDI Q4|  58|         2|          15|          72|
|           AUDI Q4|  58|         2|          16|          73|
|           AUDI Q4|  58|         2|          17|          74|
|           AUDI Q4|  58|         2|          18|          75|
|           AUDI Q4|  58|         2|          19|          76|
|           AUDI Q4|  58|         2|          20|          77|
|           AUDI Q4|  58|         2|          21|          78|
|           AUDI Q4|  58|         2|          22|          79|
|           AUDI Q4|  58|         2|          23|          80|
|           AUDI Q4|  58|         2|          24|          81|
|           AUDI Q4|  58|         2|          25|          82|
|           AUDI Q4|  58|         2|          26|          83|
|           AUDI Q4|  58|         2|          27|          84|
|           AUDI Q4|  58|         2|          28|          85|
|           AUDI Q4|  58|         2|          29|          86|
|           AUDI Q4|  58|         2|          30|          87|
|           AUDI Q4|  58|         2|          31|          88|
|           AUDI Q4|  58|         2|          32|          89|
|           AUDI Q4|  58|         2|          33|          90|
|           AUDI Q4|  58|         2|          34|          91|
|           AUDI Q4|  58|         2|          35|          92|
|           AUDI Q4|  58|         2|          36|          93|
|           AUDI Q4|  58|         2|          37|          94|
|           AUDI Q4|  58|         2|          38|          95|
|           AUDI Q4|  58|         2|          39|          96|
|           AUDI Q4|  58|         2|          40|          97|
|           AUDI Q4|  58|         2|          41|          98|
|           AUDI Q4|  58|         2|          42|          99|
|           AUDI Q4|  58|         2|          43|         100|
+------------------+----+----------+------------+------------+
only showing top 100 rows















models_mak.groupBy("generic_make_model")
        .agg(when(col("generic_make_model").contains("TESLA"),count("generic_make_model"))).show(100)
+-------------------+--------------------------------------------------------------------------------+
| generic_make_model|CASE WHEN contains(generic_make_model, TESLA) THEN count(generic_make_model) END|
+-------------------+--------------------------------------------------------------------------------+
|       HYUNDAI KONA|                                                                            NULL|
|            AUDI Q4|                                                                            NULL|
|           KIA SOUL|                                                                            NULL|
|     PORSCHE TAYCAN|                                                                            NULL|
|             BMW IX|                                                                            NULL|
|      PORSCHE MACAN|                                                                            NULL|
|         VOLVO XC40|                                                                            NULL|
|       NISSAN ARIYA|                                                                            NULL|
|      TESLA MODEL X|                                                                              31|
|        FORD RANGER|                                                                            NULL|
|      TESLA MODEL 3|                                                                             271|
|    CHEVROLET SPARK|                                                                            NULL|
|             BMW I4|                                                                            NULL|
|         HONDA CR-V|                                                                            NULL|
|      JAGUAR I-PACE|                                                                            NULL|
|        NISSAN LEAF|                                                                            NULL|
|        AUDI E-TRON|                                                                            NULL|
|            AUDI Q8|                                                                            NULL|
|           KIA NIRO|                                                                            NULL|
|      TESLA MODEL Y|                                                                             164|
|             BMW I3|                                                                            NULL|
|      HYUNDAI IONIQ|                                                                            NULL|
|       SMART FORTWO|                                                                            NULL|
|        TOYOTA RAV4|                                                                            NULL|
|          VOLVO C40|                                                                            NULL|
|            KIA EV6|                                                                            NULL|
|FORD MUSTANG MACH-E|                                                                            NULL|
|      TESLA MODEL S|                                                                              41|
+-------------------+--------------------------------------------------------------------------------+










models_mak.groupBy("generic_make_model") \
                .agg(when(~col("generic_make_model").contains("TESLA"),count("generic_make_model"))).show(100)
+-------------------+--------------------------------------------------------------------------------------+
| generic_make_model|CASE WHEN (NOT contains(generic_make_model, TESLA)) THEN count(generic_make_model) END|
+-------------------+--------------------------------------------------------------------------------------+
|       HYUNDAI KONA|                                                                                   234|
|            AUDI Q4|                                                                                    44|
|           KIA SOUL|                                                                                    58|
|     PORSCHE TAYCAN|                                                                                    41|
|             BMW IX|                                                                                    33|
|      PORSCHE MACAN|                                                                                    83|
|         VOLVO XC40|                                                                                   218|
|       NISSAN ARIYA|                                                                                    13|
|      TESLA MODEL X|                                                                                  NULL|
|        FORD RANGER|                                                                                    43|
|      TESLA MODEL 3|                                                                                  NULL|
|    CHEVROLET SPARK|                                                                                    44|
|             BMW I4|                                                                                    36|
|         HONDA CR-V|                                                                                   521|
|      JAGUAR I-PACE|                                                                                    57|
|        NISSAN LEAF|                                                                                   233|
|        AUDI E-TRON|                                                                                    57|
|            AUDI Q8|                                                                                    34|
|           KIA NIRO|                                                                                   546|
|      TESLA MODEL Y|                                                                                  NULL|
|             BMW I3|                                                                                   118|
|      HYUNDAI IONIQ|                                                                                   395|
|       SMART FORTWO|                                                                                   193|
|        TOYOTA RAV4|                                                                                   410|
|          VOLVO C40|                                                                                    11|
|            KIA EV6|                                                                                    20|
|FORD MUSTANG MACH-E|                                                                                    29|
|      TESLA MODEL S|                                                                                  NULL|
+-------------------+--------------------------------------------------------------------------------------+







models_mak.groupBy("generic_make_model") \
                .agg(when(~col("generic_make_model").contains("TESLA"),count("generic_make_model"))
                     .alias("Count")).filter(col("Count") > 1).show(100)


+-------------------+-----+
| generic_make_model|Count|
+-------------------+-----+
|       HYUNDAI KONA|  234|
|            AUDI Q4|   44|
|           KIA SOUL|   58|
|     PORSCHE TAYCAN|   41|
|             BMW IX|   33|
|      PORSCHE MACAN|   83|
|         VOLVO XC40|  218|
|       NISSAN ARIYA|   13|
|        FORD RANGER|   43|
|    CHEVROLET SPARK|   44|
|             BMW I4|   36|
|         HONDA CR-V|  521|
|      JAGUAR I-PACE|   57|
|        NISSAN LEAF|  233|
|        AUDI E-TRON|   57|
|            AUDI Q8|   34|
|           KIA NIRO|  546|
|             BMW I3|  118|
|      HYUNDAI IONIQ|  395|
|       SMART FORTWO|  193|
|        TOYOTA RAV4|  410|
|          VOLVO C40|   11|
|            KIA EV6|   20|
|FORD MUSTANG MACH-E|   29|
+-------------------+-----+





(models_mak.groupBy("generic_make_model") \
                .agg(when(~col("generic_make_model").contains("TESLA"),count("generic_make_model")).alias("Count"))
                .filter(col("Count") > 1)
                .orderBy(col("Count").desc())
                .limit(10).show(100))
+------------------+-----+
|generic_make_model|Count|
+------------------+-----+
|          KIA NIRO|  546|
|        HONDA CR-V|  521|
|       TOYOTA RAV4|  410|
|     HYUNDAI IONIQ|  395|
|      HYUNDAI KONA|  234|
|       NISSAN LEAF|  233|
|        VOLVO XC40|  218|
|      SMART FORTWO|  193|
|            BMW I3|  118|
|     PORSCHE MACAN|   83|
+------------------+-----+














