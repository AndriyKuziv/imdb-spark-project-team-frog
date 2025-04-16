from pyspark.sql.window import Window
from load_data import load_data
from pyspark.sql.functions import *
from pyspark.sql.functions import split, explode, col

data = load_data()

df_name_basics = data ['name_basics']
df_title_akas = data['title_akas']
df_title_basics = data['title_basics']
df_title_crew = data['title_crew']
df_title_episode = data['title_episode']
df_title_principals = data['title_principals']
df_title_ratings = data['title_ratings']

def run_sofiia_questions(data):
    df_title_genres = df_title_basics \
        .filter(col("genres").isNotNull()) \
        .withColumn("genre", explode(split("genres", ",")))
    df_name_professions = df_name_basics \
        .filter(col("primaryProfession").isNotNull()) \
        .withColumn("profession", explode(split("primaryProfession", ",")))

    print('='*50 + '\n')
    # Питання 1– Актори з найбільшою кількістю фільмів
    # filter, join, groupBy
    movies = df_title_basics.filter(col("titleType") == "movie").select("tconst")
    most_active_actors = df_title_principals \
        .filter(col("category") == "actor") \
        .join(movies, "tconst") \
        .groupBy("nconst") \
        .agg(countDistinct("tconst").alias("film_count")) \
        .join(df_name_basics, "nconst", "left") \
        .select("primaryName", "film_count") \
        .orderBy(col("film_count").desc())

    print("1. Актори з найбільшою кількістю фільмів:")
    most_active_actors.show(20)
    print(f"Кількість рядків результуючої таблиці: {most_active_actors.count()}")
    print('\n' + '=' * 50+ '\n')
    # Питання 2 – Кількість фільмів жанрів по роках
    # filter, groupBy
    genre_years = df_title_basics \
        .filter(col("titleType") == "movie") \
        .select("startYear", explode(split("genres", ",")).alias("genre")) \
        .groupBy("startYear", "genre") \
        .count() \
        .orderBy("startYear", "count", ascending=False)

    print("2. Кількість фільмів усіх жанрів по роках")
    genre_years.show(20)
    print(f"Кількість рядків результуючої таблиці: {genre_years.count()}")
    print('\n' + '=' * 50+ '\n')
    # Питання 3 – Регіони з найбільшою кількістю локалізацій
    # groupBy, filter
    top_regions = df_title_akas \
        .filter(col("region").isNotNull()) \
        .groupBy("region") \
        .agg(countDistinct("titleId").alias("unique_titles")) \
        .orderBy(col("unique_titles").desc())

    print("3. Регіони з найбільшою кількістю локалізацій")
    top_regions.show(20)
    print(f"Кількість рядків результуючої таблиці: {top_regions.count()}")
    print('\n' + '=' * 50+ '\n')
    # Питання 4 – Серіали з найвищим середнім рейтингом (і середньою к-стю голосів > 1000)
    # join, filter, groupBy
    episodes_rated = df_title_episode.join(df_title_ratings, "tconst")
    series_stats = episodes_rated \
        .groupBy("parentTconst") \
        .agg(
        avg("averageRating").alias("avg_rating"),
        avg("numVotes").alias("avg_votes"),
        count("*").alias("episode_count")
    ) \
        .filter(col("avg_votes") > 1000)

    top_series = series_stats \
        .join(df_title_basics, series_stats["parentTconst"] == df_title_basics["tconst"]) \
        .select("primaryTitle", "avg_rating", "avg_votes", "episode_count") \
        .orderBy(col("avg_rating").desc())

    print("4. Серіали з найвищим середнім рейтингом (і середньою к-стю голосів > 1000):")
    top_series.show(20, truncate=False)
    print(f"Кількість рядків результуючої таблиці: {top_series.count()}")
    print('\n' + '=' * 50+ '\n')
    # Питання 5 – Топ-5 фільмів у кожному жанрі (з кількістю голосів > 10000)
    # filter, join, window
    genre_rated = df_title_genres \
        .join(df_title_ratings, "tconst") \
        .filter(col("numVotes") > 10000)

    w_genre_rank = Window.partitionBy("genre").orderBy(col("averageRating").desc())

    top5_per_genre = genre_rated \
        .withColumn("rank", row_number().over(w_genre_rank)) \
        .filter(col("rank") <= 5)

    print("5. Топ-5 фільмів у кожному жанрі (з кількістю голосів > 10000):")
    top5_per_genre.select("genre", "primaryTitle", "averageRating", "numVotes", "startYear").show(20, truncate=False)
    print(f"Кількість рядків результуючої таблиці: {top5_per_genre.count()}")
    print('\n' + '=' * 50 + '\n')
    # Питання 6 – Найраніший фільм у кожному жанрі з іменами режисерів
    # filter, window, join, groupBy
    genre_with_year = df_title_genres \
        .filter((col("startYear").isNotNull()) & (col("startYear") > 0))
    w_earliest = Window.partitionBy("genre").orderBy(col("startYear").asc())
    earliest_per_genre = genre_with_year \
        .withColumn("rank", row_number().over(w_earliest)) \
        .filter(col("rank") == 1)

    earliest_with_directors = earliest_per_genre \
        .join(df_title_crew, "tconst", "left")
    directors_exploded = earliest_with_directors \
        .withColumn("director", explode(split("directors", ",")))

    directors_named = directors_exploded \
        .join(df_name_basics.select("nconst", "primaryName"),
              directors_exploded["director"] == df_name_basics["nconst"], "left")
    earliest_per_genre_final = directors_named \
        .groupBy("genre", "primaryTitle", "startYear", "runtimeMinutes") \
        .agg(concat_ws(", ", collect_list("primaryName")).alias("directors"))
    earliest_per_genre_final_sorted = earliest_per_genre_final.orderBy("genre")
    print("6. Найраніший фільм у кожному жанрі з іменами режисерів:")
    earliest_per_genre_final_sorted.select("genre", "primaryTitle", "startYear", "directors").show(20, truncate=False)
    print(f"Кількість рядків результуючої таблиці: {earliest_per_genre_final_sorted.count()}")
    print('\n' + '=' * 50)

    return {
        "most_active_actors": most_active_actors,
        "genre_years": genre_years,
        "top_regions": top_regions,
        "top_series": top_series,
        "top5_per_genre": top5_per_genre,
        "earliest_per_genre": earliest_per_genre_final_sorted
    }


def save_sofiia_questions(results):
    results["most_active_actors"] \
        .write.option("header", "true").csv("output/most_active_actors.csv", mode="overwrite")

    results["genre_years"] \
        .write.option("header", "true").csv("output/genre_years.csv", mode="overwrite")

    results["top_regions"] \
        .write.option("header", "true").csv("output/top_regions.csv", mode="overwrite")

    results["top_series"] \
        .write.option("header", "true").csv("output/top_series.csv", mode="overwrite")

    results["top5_per_genre"].select("genre", "primaryTitle", "averageRating", "numVotes", "startYear") \
        .write.option("header", "true").csv("output/top5_per_genre.csv", mode="overwrite")

    results["earliest_per_genre_final_sorted"].select("genre", "primaryTitle", "startYear", "directors") \
        .write.option("header", "true").csv("output/earliest_per_genre.csv", mode="overwrite")


run_sofiia_questions(data)
# results = run_sofiia_questions(data)
# save_sofiia_questions (results)
