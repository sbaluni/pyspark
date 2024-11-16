#!/usr/bin/env python3
#
#   User                    Date(dd/mm/yyyy)     Description 
#   -------------------------------------------------------------------------------------
#   Suryakant Baluni        17/02/2024           Read a text file that contains 3 columns
#                                                comma saperated (ID, order date, user ID, order status).
#                                                Find which user has maximum number of CLOSED orders.
#

from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    config('spark.ui.port', '0'). \
    enableHiveSupport(). \
    getOrCreate()

base_rdd = spark.sparkContext.textFile('/home/sbaluni/Personal/work/pyspark/prg_03_orders_rdd/orders_data.txt')

# Filter the closed orders
fil_closed_orders_rdd = base_rdd.filter(lambda x : (x.split(",")[3] == 'CLOSED'))

# Map based on User ID
map_closed_orders_rdd = fil_closed_orders_rdd.map(lambda x : (x.split(",")[2], 1))

# Aggregate results based on User ID
red_closed_orders = map_closed_orders_rdd.reduceByKey(lambda x, y : x + y )

# Sort the results in DESC order of number of closed orders
sort_closed_orders = red_closed_orders.sortBy(lambda x : x[1], False)

# Get first record. 
print(sort_closed_orders.take(1))

# Or we can dump entire result set to a file, and let the consumer decide how they want to consume the data.
#sort_closed_orders.saveAsTextFile('/home/sbaluni/Personal/work/pyspark/prg_03_orders_rdd/output_04')