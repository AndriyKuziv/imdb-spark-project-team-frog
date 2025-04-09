from pyspark.sql import SparkSession
from pyspark.sql.types import *

def load_data():
    spark = SparkSession.builder \
        .appName("IMDb Data Loader") \
        .getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    name_basics_schema = StructType([
        StructField("nconst", StringType(), True),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", IntegerType(), True),
        StructField("deathYear", IntegerType(), True),
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True),
    ])

    title_akas_schema = StructType([
        StructField("titleId", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("region", StringType(), True),
        StructField("language", StringType(), True),
        StructField("types", StringType(), True),
        StructField("attributes", StringType(), True),
        StructField("isOriginalTitle", IntegerType(), True),
    ])

    title_basics_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", IntegerType(), True),
        StructField("startYear", IntegerType(), True),
        StructField("endYear", IntegerType(), True),
        StructField("runtimeMinutes", IntegerType(), True),
        StructField("genres", StringType(), True),
    ])

    title_crew_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("directors", StringType(), True),
        StructField("writers", StringType(), True),
    ])

    title_episode_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("parentTconst", StringType(), True),
        StructField("seasonNumber", IntegerType(), True),
        StructField("episodeNumber", IntegerType(), True),
    ])

    title_principals_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("nconst", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True),
    ])

    title_ratings_schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("averageRating", FloatType(), True),
        StructField("numVotes", IntegerType(), True),
    ])

    df_name_basics = spark.read.option("header", "true").option("sep", "\t") \
        .option("nullValue", "\\N").schema(name_basics_schema).csv("name.basics.tsv")

    df_title_akas = spark.read.option("header", "true").option("sep", "\t") \
        .option("nullValue", "\\N").schema(title_akas_schema).csv("title.akas.tsv")

    df_title_basics = spark.read.option("header", "true").option("sep", "\t") \
        .option("nullValue", "\\N").schema(title_basics_schema).csv("title.basics.tsv")

    df_title_crew = spark.read.option("header", "true").option("sep", "\t") \
        .option("nullValue", "\\N").schema(title_crew_schema).csv("title.crew.tsv")

    df_title_episode = spark.read.option("header", "true").option("sep", "\t") \
        .option("nullValue", "\\N").schema(title_episode_schema).csv("title.episode.tsv")

    df_title_principals = spark.read.option("header", "true").option("sep", "\t") \
        .option("nullValue", "\\N").schema(title_principals_schema).csv("title.principals.tsv")

    df_title_ratings = spark.read.option("header", "true").option("sep", "\t") \
        .option("nullValue", "\\N").schema(title_ratings_schema).csv("title.ratings.tsv")

    return {
        "spark": spark,
        "name_basics": df_name_basics,
        "title_akas": df_title_akas,
        "title_basics": df_title_basics,
        "title_crew": df_title_crew,
        "title_episode": df_title_episode,
        "title_principals": df_title_principals,
        "title_ratings": df_title_ratings,
    }
