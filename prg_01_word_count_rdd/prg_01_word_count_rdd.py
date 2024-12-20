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

base_rdd = spark.sparkContext.textFile('/home/sbaluni/Personal/work/pyspark/prg_01_word_count_rdd/data.txt')

# Split each line by space and create a rdd with flatten data.
# Also converting each word to lower case for accurate results.
fmap_words_rdd = base_rdd.flatMap(lambda x : x.lower().split(" "))

# Map each word with 1 count 
map_words_rdd = fmap_words_rdd.map(lambda x : (x,1))

# Aggregate the result for each word as key
red_words_rdd = map_words_rdd.reduceByKey(lambda x,y : x+y)

#print(red_words_rdd.collect())
red_words_rdd.saveAsTextFile('/home/sbaluni/Personal/work/pyspark/prg_01_word_count_rdd/output')


