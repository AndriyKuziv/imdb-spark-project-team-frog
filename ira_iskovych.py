from load_data import load_data
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import avg
from pyspark.sql.functions import explode, split
from pyspark.sql.functions import col, expr, count


dfs = load_data()

df_title_ratings = dfs["title_ratings"]
df_title_basics = dfs["title_basics"]
df_title_crew = dfs["title_crew"]
df_name_basics = dfs["name_basics"]
df_title_principals = dfs["title_principals"]
df_title_episode = dfs["title_episode"]

"""
НЕПОВНОЛІТНІ АКТОРИ
"""

actors_films = df_title_crew \
    .join(df_title_basics, on="tconst", how="inner") \
    .join(df_name_basics, df_title_crew["directors"] == df_name_basics["nconst"], how="inner")

actors_films_with_age = actors_films \
    .withColumn("actor_age_on_shooting", (col("startYear") - col("birthYear")).cast("int"))

minor_actors_films = actors_films_with_age \
    .filter(
        (col("actor_age_on_shooting") < 18) &
        (col("actor_age_on_shooting") >= 0) &
        (col("primaryProfession").contains("actor") | col("primaryProfession").contains("actress"))
    ) \
    .select("primaryName", "primaryTitle", "startYear", "actor_age_on_shooting", "primaryProfession") \
    .orderBy(col("actor_age_on_shooting").asc())


minor_actors_films.show(20, truncate=False)

print(f"Загальна кількість неповнолітніх акторів на рік зйомки: {minor_actors_films.count()}")



"""
Серіали з більше ніж 100 серіями(у висновки що не все додано)
"""

episodes = df_title_episode.join(
    df_title_basics.filter(col("titleType") == "tvEpisode"),
    on="tconst",
    how="inner"
)

episode_counts = episodes.groupBy("parentTconst").agg(count("*").alias("episode_count"))

long_series = episode_counts.filter(col("episode_count") > 100)

series_titles = long_series.join(
    df_title_basics.select("tconst", "primaryTitle"),
    long_series["parentTconst"] == df_title_basics["tconst"],
    how="left"
).select("primaryTitle", "episode_count")

series_titles.orderBy(col("episode_count").desc()).show(20, truncate=False)

print(f"Загальна кількість серіалів з понад 100 епізодами: {series_titles.count()}")



"""
Живі актори за 90(недостовірність датасету)
"""

current_year = 2025

old_alive_actors = df_name_basics \
    .filter(
        (col("deathYear").isNull()) &
        (col("birthYear").isNotNull()) &
        ((current_year - col("birthYear").cast("int")) > 90) &
        (
            col("primaryProfession").contains("actor") |
            col("primaryProfession").contains("actress")
        )
    ) \
    .withColumn("age", (expr(f"{current_year} - birthYear").cast("int"))) \
    .select("primaryName", "age", "primaryProfession") \
    .orderBy(col("age").desc())  # Сортуємо за віком від старших до молодших

old_alive_actors.show(20, truncate=False)

print(f"Загальна кількість живих акторів старше 90: {old_alive_actors.count()}")



"""
 Найкращий фільм режисера
"""

df_director_films = df_title_crew \
    .filter((df_title_crew["directors"].isNotNull()) & (~df_title_crew["directors"].contains(","))) \
    .withColumnRenamed("directors", "nconst")

df_joined = df_director_films \
    .join(df_title_basics.select("tconst", "primaryTitle"), on="tconst") \
    .join(df_title_ratings.select("tconst", "averageRating"), on="tconst")

windowSpec = Window.partitionBy("nconst").orderBy(df_joined["averageRating"].desc())

top_film_per_director = df_joined \
    .withColumn("rank", row_number().over(windowSpec)) \
    .filter("rank = 1")

result = top_film_per_director \
    .join(df_name_basics.select("nconst", "primaryName"), on="nconst") \
    .select("primaryName", "primaryTitle") \
    .orderBy("primaryName")

result.show(20, truncate=False)

print(f"Загальна кількість записів у вибірці: {result.count()}")



"""
Виводимо топ 3 фільми жанру драма за роками
"""

movies = df_title_basics.filter(
    (df_title_basics["titleType"] == "movie") &
    df_title_basics["primaryTitle"].isNotNull() &
    df_title_basics["startYear"].isNotNull() &
    df_title_basics["genres"].contains("Drama")
)

movies_with_ratings = movies.join(df_title_ratings, on="tconst", how="inner") \
    .filter(df_title_ratings["averageRating"].isNotNull())

year_window = Window.partitionBy("startYear")
movies_with_avg = movies_with_ratings.withColumn(
    "avg_year_rating",
    avg("averageRating").over(year_window)
)

above_avg = movies_with_avg.filter("averageRating > avg_year_rating")

ranking_window = Window.partitionBy("startYear").orderBy(above_avg["averageRating"].desc())
ranked = above_avg.withColumn("rank", row_number().over(ranking_window)).filter("rank <= 3")

final_result = ranked.select("primaryTitle", "startYear", "averageRating") \
    .orderBy("startYear", "averageRating", ascending=[False, False])

final_result.show(20)

print(f"Загальна кількість драм-фільмів у вибірці: {final_result.count()}")



"""
Фільми з рейтингом вище середнього для їхнього року
"""

movies = df_title_basics.filter(
    (df_title_basics["titleType"] == "movie") &
    df_title_basics["primaryTitle"].isNotNull() &
    df_title_basics["startYear"].isNotNull()
)

movies_with_ratings = movies.join(df_title_ratings, on="tconst", how="inner") \
    .filter(df_title_ratings["averageRating"].isNotNull())

windowSpec = Window.partitionBy("startYear")
movies_with_avg = movies_with_ratings.withColumn(
    "avg_year_rating",
    avg("averageRating").over(windowSpec)
)

above_avg = movies_with_avg.filter("averageRating > avg_year_rating")

result = above_avg.select("primaryTitle", "startYear", "averageRating", "avg_year_rating") \
    .orderBy("averageRating", ascending=False)

result.show(20)

print(f"Загальна кількість фільмів у вибірці: {result.count()}")
