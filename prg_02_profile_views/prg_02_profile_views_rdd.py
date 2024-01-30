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

base_rdd = spark.sparkContext.textFile('/home/sbaluni/Personal/work/pyspark/prg_02_profile_views/data.txt')
rdd2 = base_rdd.map(lambda x : x.title().split(",")[2])
rdd3 = rdd2.map(lambda x : (x,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)

#print(rdd4.collect())
rdd4.saveAsTextFile('/home/sbaluni/Personal/work/pyspark/prg_02_profile_views/output')


