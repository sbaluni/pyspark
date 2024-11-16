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

base_rdd = spark.sparkContext.textFile('/home/sbaluni/Personal/work/pyspark/prg_03_orders_rdd/orders_data.txt')

# Map the rdd based on status
map_orders_rdd = base_rdd.map(lambda x : (x.split(",")[3], 1))

# Aggregate the results for each status
red_orders_rdd = map_orders_rdd.reduceByKey(lambda x, y : x + y)

# Sort the output by number of orders in each status.
sort_orders_rdd = red_orders_rdd.sortBy(lambda x: x[1])

# Sort Descending - Pass False
# sort_orders_rdd = red_orders_rdd.sortBy(lambda x: x[1], False)

#print(sort_orders_rdd.collect())
sort_orders_rdd.saveAsTextFile('/home/sbaluni/Personal/work/pyspark/prg_03_orders_rdd/output_01')