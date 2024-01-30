#!/usr/bin/env python3
#
#   User                    Date(dd/mm/yyyy)     Description 
#   -------------------------------------------------------------------------------------
#   Suryakant Baluni        29/12/2023           Read a text file that contains 3 columns
#                                                comma saperated (ID, order date, user ID, order status).
#                                                Count the number of orders under each status.
#

from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    config('spark.ui.port', '0'). \
    enableHiveSupport(). \
    getOrCreate()

base_rdd = spark.sparkContext.textFile('/home/sbaluni/Personal/work/pyspark/prg_03_orders/orders_data.txt')

rdd2 = base_rdd.map(lambda x : (x.split(",")[3], 1))
rdd3= rdd2.reduceByKey(lambda x, y : x + y)

#print(rdd3.collect())
rdd3.saveAsTextFile('/home/sbaluni/Personal/work/pyspark/prg_03_orders/output_01')


