from ctypes import cast
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, format_number, concat, lit, avg, count, desc, sum as F_sum, round as spark_round, when, create_map, expr
from functools import reduce
from itertools import chain
from operator import itemgetter
from pyspark.sql.functions import count as spark_count


spark = SparkSession.builder.appName("Trump").getOrCreate()
données_trump = spark.read.csv("trump_insult_tweets_2014_to_2021.csv", header=True, inferSchema=True, multiLine=True, escape="\"")

totalLignes = données_trump.count()


# Comptes insultés
print("##################################################")
targets = données_trump.select("target")
targetCounts = targets.groupBy("target").count()
sortedTargetCounts = targetCounts.orderBy(col("count").desc())
top10Targets = sortedTargetCounts.limit(10)
top10Targets.show()
print("##################################################")



# Insultes utilisées
print("##################################################")
insults = données_trump.select("insult")
insultCounts = insults.groupBy("insult").count()
sortedInsultsCounts = insultCounts.orderBy(col("count").desc())
top10Insults = sortedInsultsCounts.limit(10)
top10Insults.show()
print("##################################################")

# Insulte Joe Bidon

# MEXICO/CHINA/CRNV
mexico = données_trump.filter(col("insult").contains("Mexico"))
mexico_count = mexico.count()
china = données_trump.filter(col("insult").contains("China"))
china_count = china.count()
corona = données_trump.filter(col("insult").contains("coronavirus"))
corona_count = corona.count()

print("##################################################")
print("Apparition du mot Mexico / China / Coronavirus")
print(f"Mexico: {mexico_count}")
print(f"China: {china_count}")
print(f"Coronavirus: {corona_count}")
print("##################################################")

# TWEET/P6M



