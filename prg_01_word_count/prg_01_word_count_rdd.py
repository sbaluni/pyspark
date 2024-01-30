#!/usr/bin/env python3
#
#   User                    Date(dd/mm/yyyy)     Description 
#   ----------------------------------------------------------------------------------
#   Suryakant Baluni        29/12/2023           Read a text file and count frequency 
#                                                of each word present in file. Perform 
#                                                case-insensitive match.
#

from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    config('spark.ui.port', '0'). \
    enableHiveSupport(). \
    getOrCreate()

base_rdd = spark.sparkContext.textFile('/home/sbaluni/Personal/work/pyspark/prg_01_word_count/data.txt')
rdd2 = base_rdd.flatMap(lambda x : x.lower().split(" "))
rdd3 = rdd2.map(lambda x : (x,1))
rdd4 = rdd3.reduceByKey(lambda x,y : x+y)

#print(rdd4.collect())
rdd4.saveAsTextFile('/home/sbaluni/Personal/work/pyspark/prg_01_word_count/output')


