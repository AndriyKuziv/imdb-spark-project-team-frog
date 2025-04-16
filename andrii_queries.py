from load_data import load_data
from pyspark.sql.functions import *

data = load_data()

spark = data["spark"]
dfs = {key: value for key, value in data.items() if key != "spark"}


def getGermanRegionTitles():
    print(f"-" * 21 + "Тайтли у німецькому регіоні" + "-" * 21)

    title_akas = dfs["title_akas"]

    germanTitles = title_akas.filter(title_akas.region == "DE") \
        .select(
        col("titleId"),
        col("title"),
        col("region"),
        col("language")
    )

    germanTitles.show(20, truncate=False)
    print("Кількість тайтлів у німецькому регіоні:", germanTitles.count())


def getGermanMoviesFromYear(border_year):
    print(f"-" * 21 + f"Німецькомовні фільми у німецькому регіоні, які виходили з {border_year} року" + "-" * 21)

    title_basics = dfs["title_basics"]
    title_akas = dfs["title_akas"]

    title_basics = title_basics.filter(title_basics.titleType == "movie")

    title_akas = title_akas.filter((title_akas.language == "de") & (title_akas.region == "DE"))

    movies = title_basics.join(title_akas, title_basics.tconst == title_akas.titleId, how="inner") \
        .select(
        title_basics["tconst"],
        title_akas["title"],
        title_akas["region"],
        title_akas["language"],
        title_basics["startYear"].alias("year"),
        title_basics["genres"])

    movies = movies.filter(movies.year >= border_year).orderBy(movies.year)

    movies.show(20, truncate=False)
    print(f"Кількість німецькомовних фільмів у німецькому регіоні, які виходили з {border_year} року:", movies.count())


def getWritersWithAvgRatingFrom(border_rating):
    print(f"-" * 21 + f"Сценаристи, у яких середній рейтинг робіт не менше {border_rating}" + "-" * 21)

    title_basics = dfs["title_basics"]
    title_ratings = dfs["title_ratings"]
    title_crew = dfs["title_crew"]
    name_basics = dfs["name_basics"]

    title_ratings = title_ratings.filter(title_ratings.numVotes >= 1000)

    titles = title_basics.join(title_ratings, on="tconst", how="inner") \
        .select(
        title_basics["tconst"],
        title_basics["titleType"],
        title_basics["primaryTitle"],
        title_basics["startYear"],
        title_basics["genres"],
        title_ratings["averageRating"],
        title_ratings["numVotes"]
    )
    writers = title_crew.join(name_basics, title_crew.writers == name_basics.nconst, how="inner") \
        .select(
        title_crew["tconst"],
        name_basics["nconst"],
        name_basics["primaryName"].alias("writer")
    )

    avgWriterRating = titles.join(writers, on="tconst", how="inner") \
        .groupBy("writer").agg(avg("averageRating").alias("avgWriterRating")) \
        .filter(col("avgWriterRating") >= border_rating).orderBy(col("avgWriterRating"))

    avgWriterRating.show(20, truncate=False)
    print(f"Кількість сценаристів, у яких середній рейтинг робіт не менше {border_rating}:", avgWriterRating.count())


def getCountriesLongestSeries():
    print(f"-" * 21 + "Найбільша кількість серій в одному сезоні в кожному з регіонів" + "-" * 21)

    title_basics = dfs["title_basics"]
    title_episode = dfs["title_episode"]
    title_akas = dfs["title_akas"]

    episodes = title_basics.filter(col("titleType") == "tvEpisode") \
        .join(title_akas, title_basics.tconst == title_akas.titleId, how="inner") \
        .filter(col("region") != "NULL")

    episodesWithNumbers = episodes.join(title_episode, on="tconst", how="inner")

    groupedByRegion = episodesWithNumbers.groupBy("region").agg(max("episodeNumber").alias("maxEpisodeNumber")) \
        .orderBy(col("maxEpisodeNumber"), ascending=False)

    groupedByRegion.show(20, truncate=False)
    print(f"Кількість регіонів, в яких виходив хоча б один серіал:", groupedByRegion.count())


def getEpisodesWithRatingFrom(border_rating):
    print(f"-" * 21 + f"Епізоди, в яких середній рейтинг не менше {border_rating}" + "-" * 21)

    title_basics = dfs["title_basics"]
    title_ratings = dfs["title_ratings"]

    title_ratings = title_ratings.filter(title_ratings.numVotes >= 1000)

    title_basics = title_basics.filter(col("titleType") == "tvEpisode")

    filteredEpisodes = title_basics.join(title_ratings, on="tconst", how="inner") \
        .filter(title_ratings.averageRating >= border_rating).orderBy(col("averageRating")) \
        .select(
        col("primaryTitle").alias("episodeTitle"),
        col("startYear").alias("releaseYear"),
        col("genres"),
        col("averageRating"),
        col("numVotes"),
    )

    filteredEpisodes.show(20, truncate=False)
    print(f"Кількість епізодів, в яких середній рейтинг не менше {border_rating}:", filteredEpisodes.count())


def getTitlesWithSameDirectorWriter():
    print(f"-" * 21 + "Тайтли, у яких одна людина виконує роль сценариста та режисера" + "-" * 21)

    title_basics = dfs["title_basics"]
    title_crew = dfs["title_crew"]
    name_basics = dfs["name_basics"]

    title_crew = title_crew.filter((title_crew.writers != "NULL") & (title_crew.directors == title_crew.writers))

    authors = title_crew.join(name_basics, name_basics.nconst == title_crew.writers, how="inner") \
        .select(
        title_crew["tconst"],
        title_crew["directors"],
        title_crew["writers"],
        name_basics["primaryName"].alias("authorName"),
    )

    titles = title_basics.join(authors, on="tconst", how="inner").orderBy(col("startYear")) \
        .select(
        col("tconst"),
        col("primaryTitle").alias("title"),
        col("startYear").alias("releaseYear"),
        col("genres"),
        col("directors"),
        col("writers"),
        col("authorName")
    )

    titles.show(20, truncate=False)
    print("Кількість тайтлів, у яких одна людина виконує роль сценариста та режисера:", titles.count())


def run_andrii_queries():
    getGermanRegionTitles()
    getGermanMoviesFromYear(2010)
    getTitlesWithSameDirectorWriter()
    getCountriesLongestSeries()
    getEpisodesWithRatingFrom(8.1)
    getWritersWithAvgRatingFrom(8.1)
