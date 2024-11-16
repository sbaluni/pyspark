#!/usr/bin/env python3
#
#   User                    Date(dd/mm/yyyy)     Description 
#   -------------------------------------------------------------------------------------
#   Suryakant Baluni        17/02/2024           Read a text file that contains 3 columns
#                                                comma saperated (ID, order date, user ID, order status).
#                                                Find distinct count of customers who placed at least 1 order.
#

from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    config('spark.ui.port', '0'). \
    enableHiveSupport(). \
    getOrCreate()

base_rdd = spark.sparkContext.textFile('/home/sbaluni/Personal/work/pyspark/prg_03_orders_rdd/orders_data.txt')

# Map based on customer ID
map_user_rdd = base_rdd.map(lambda x : x.split(",")[2])

# Get unique users
map_unique_cust_rdd = map_user_rdd.distinct()

# Store count of map_unique_cust_rdd to a variable
total_unique_cust = map_unique_cust_rdd.count()

print('Total Unique Customers who placed at least one order: ', total_unique_cust)