

df.select("*","state").distinct().show(10)
"""
+-----------+----------+---------+--------------------+------------+--------------+------------+-----+--------+-----+
|customer_id|first_name|last_name|               email|phone_number|       address|        city|state|zip_code|state|
+-----------+----------+---------+--------------------+------------+--------------+------------+-----+--------+-----+
|         49|     Elena|     Gray| elena.gray@zoho.com|    555-0049|   4646 Elm St|     Detroit|   MI|   48202|   MI|
|         40|  Benjamin|    Evans|benjamin.evans@zo...|    555-0040|   3737 Elm St|      Denver|   CO|   80202|   CO|
|         35|    Harper| Mitchell|harper.mitchell@z...|    555-0035|3232 Walnut St|Philadelphia|   PA|   19101|   PA|
|          1|      John|    Smith|john.smith@domain...|    555-0001|    123 Elm St| Springfield|   IL|   62701|   IL|
|         20|    Elijah|   Garcia|elijah.garcia@zoh...|    555-0020|   1717 Oak St|     Detroit|   MI|   48201|   MI|
|          8|     James| Martinez|james.martinez@li...|    555-0008| 505 Walnut St|  Des Moines|   IA|   50301|   IA|
|         37|     Avery|    Allen|avery.allen@fastm...|    555-0037| 3434 Maple St|   Charlotte|   NC|   28202|   NC|
|         28|       Leo|     King|leo.king@outlook.com|    555-0028| 2525 Maple St|   Las Vegas|   NV|   89101|   NV|
|          6|     Alice|   Miller|alice.miller@aol.com|    555-0006|  303 Cedar St|     Oakland|   CA|   94601|   CA|
|         41|  Penelope|  Roberts|penelope.roberts@...|    555-0041| 3838 Birch St|   Las Vegas|   NV|   89102|   NV|
+-----------+----------+---------+--------------------+------------+--------------+------------+-----+--------+-----+
only showing top 10 rows
"""





