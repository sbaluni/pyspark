#!/usr/bin/env python3
#
#   User                    Date(dd/mm/yyyy)     Description 
#   -------------------------------------------------------------------------------------
#   Suryakant Baluni        29/12/2023           Read a text file that contains 3 columns
#                                                comma saperated (ID, WHO Viewed, Whose Profile.)
#                                                For each user calculate number of profile views.
#

from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    config('spark.ui.port', '0'). \
    enableHiveSupport(). \
    getOrCreate()

base_rdd = spark.sparkContext.textFile('/home/sbaluni/Personal/work/pyspark/prg_02_profile_views_rdd/data.txt')

# Map based on whose profile was viewed.
map_user_rdd = base_rdd.map(lambda x : (x.title().split(",")[2], 1))

# Aggregate results for each user
red_users_rdd = map_user_rdd.reduceByKey(lambda x,y : x+y)

#print(red_users_rdd.collect())
red_users_rdd.saveAsTextFile('/home/sbaluni/Personal/work/pyspark/prg_02_profile_views_rdd/output')


