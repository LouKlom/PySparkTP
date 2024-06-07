from ctypes import cast
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, format_number, concat, lit, avg, count, desc, sum as F_sum, round as spark_round, when, create_map, expr
from functools import reduce
from itertools import chain
from operator import itemgetter
from pyspark.sql.functions import count as spark_count

spark = SparkSession.builder.appName("Netflix").getOrCreate()
données_netflix = spark.read.csv("netflix_titles.csv", header=True, inferSchema=True, multiLine=True, escape="\"")


# Réalisateurs les plus prolifiques
print("##################################################")
films_par_réalisateur = données_netflix.select("director", "title")
films_comptés = films_par_réalisateur.groupBy("director").agg(count("title").alias("nombre_films"))
réalisateurs_prolifiques = films_comptés.orderBy(desc("nombre_films"))
réalisateurs_prolifiques.show()
print("##################################################")



# Pourc par Pays ======> ATTENTION ERREUR SEGMENTATION
print("##################################################")
pays_de_production = données_netflix.select("country")
films_par_pays_cleaned = pays_de_production.withColumn("nombre_films_int", cast(col("nombre_films").cast("int"), "int")).filter(col("nombre_films_int").isNotNull())
films_par_pays = films_par_pays_cleaned.groupBy("country").agg(count("country").alias("nombre_films"))
films_par_pays_avec_pourcentage = films_par_pays.withColumn("pourcentage", round((col("nombre_films") / nombre_total_films) * 100, 2))
pays_triés = films_par_pays_avec_pourcentage.orderBy(desc("pourcentage"))
pays_triés_top10 = pays_triés.orderBy(desc("pourcentage")).limit(10)
pays_triés_top10.show()
print("##################################################")



"""
# Film le plus long / Moyenne ======== NON FONCTIONNEL
print("##################################################")
films_avec_durée = données_netflix.select("release_year", "duration", "title")
films_avec_durée_minutes = films_avec_durée.withColumn("duration_minutes", col("duration").cast("int"))
durée_moyenne_films = films_avec_durée_minutes.agg(avg("duration_minutes")).alias("durée_moyenne_minutes").collect()[0][0]
film_le_plus_long = films_avec_durée_minutes.orderBy(desc("duration_minutes")).limit(1)
film_le_plus_court = films_avec_durée_minutes.orderBy("duration_minutes").limit(1)
durée_moyenne = films_avec_durée_minutes.agg(avg("duration_minutes")).alias("durée_moyenne_minutes").collect()[0][0]  # Get the value of 'durée_moyenne_minutes'
print(f"Durée moyenne des films: {durée_moyenne} minutes")
film_le_plus_long.show(truncate=False)
film_le_plus_court.show(truncate=False)
print("##################################################")
"""


print("##################################################")
collaborations = données_netflix.select("director", "cast").explode("cast").withColumnRenamed("cast", "actor").groupBy("director", "actor").count().sort(desc("count"))

top_collaboration = collaborations.limit(1).collect()[0]
top_director = top_collaboration[0]
top_actor = top_collaboration[1]
print(f"Le duo réalisateur-acteur avec le plus de collaborations ({top_collaboration[2]} films) est :")
print(f"- Réalisateur: {top_director}")
print(f"- Acteur: {top_actor}")
print("##################################################")