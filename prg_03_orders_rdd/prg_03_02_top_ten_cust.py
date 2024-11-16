#!/usr/bin/env python3
#
#   User                    Date(dd/mm/yyyy)     Description 
#   -------------------------------------------------------------------------------------
#   Suryakant Baluni        17/02/2024           Read a text file that contains 3 columns
#                                                comma saperated (ID, order date, user ID, order status).
#                                                Find top 10 customers who placed orders.
#

from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    config('spark.ui.port', '0'). \
    enableHiveSupport(). \
    getOrCreate()

base_rdd = spark.sparkContext.textFile('/home/sbaluni/Personal/work/pyspark/prg_03_orders_rdd/orders_data.txt')

# Map the rdd based on customer ID
map_cust_rdd = base_rdd.map(lambda x: (x.split(",")[2], 1))

# Aggregate the results for each customer ID
red_cust_rdd = map_cust_rdd.reduceByKey(lambda x, y : x + y)

# Sort the aggregated results on number of orders DESC
sort_cust_rdd = red_cust_rdd.sortBy(lambda x : x[1], False)

# Top 10
#sort_cust_rdd.take(10)

# Store all results in file. Consumers of file can get as many top user as needed.
sort_cust_rdd.saveAsTextFile('/home/sbaluni/Personal/work/pyspark/prg_03_orders_rdd/output_02')
