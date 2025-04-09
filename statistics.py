from load_data import load_data
from pyspark.sql.functions import *
import matplotlib.pyplot as plt

data = load_data()

spark = data["spark"]
dfs = {key: value for key, value in data.items() if key != "spark"}

###
print("----------------------")
print("Number of rows in datasets:")
for key, df in dfs.items():
    print(f"- {key}", df.count())
print("----------------------\n")

###
title_basics = dfs["title_basics"]
orig_movies = title_basics.filter(title_basics.titleType == "movie")
print("----------------------")
print("Unique movies in dataset title_basics:", orig_movies.count())
print("----------------------\n")

# ###
df_split = orig_movies.withColumn("genresArray", split(orig_movies.genres, ","))

df_exploded = df_split.select(df_split.originalTitle, explode(df_split.genresArray).alias("genre"))

genre_counts = df_exploded.groupBy("genre").count()

genre_counts.show()

data_for_plot = genre_counts.collect()

genres = [row["genre"] for row in data_for_plot]
counts = [row["count"] for row in data_for_plot]

plt.figure(figsize=(10, 6))
plt.bar(genres, counts, color="skyblue")
plt.xlabel("Genre")
plt.ylabel("Count")
plt.title("Number of movies by the genre")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

###
title_basics = dfs["title_basics"]
movies = title_basics.filter(title_basics.titleType == "movie")
title_ratings = dfs["title_ratings"]
movies_with_rating = movies.join(title_ratings, on="tconst", how="inner") \
    .select(
    movies["originalTitle"].alias("Title"),
    movies["genres"].alias("Genres"),
    title_ratings["averageRating"].alias("Rating")
)

movies_with_rating = movies_with_rating.withColumn("RatingRange",
                                                   when((col("Rating") >= 1) & (col("Rating") < 2), "1-2")
                                                   .when((col("Rating") >= 2) & (col("Rating") < 3), "2-3")
                                                   .when((col("Rating") >= 3) & (col("Rating") < 4), "3-4")
                                                   .when((col("Rating") >= 4) & (col("Rating") < 5), "4-5")
                                                   .when((col("Rating") >= 5) & (col("Rating") < 6), "5-6")
                                                   .when((col("Rating") >= 6) & (col("Rating") < 7), "6-7")
                                                   .when((col("Rating") >= 7) & (col("Rating") < 8), "7-8")
                                                   .when((col("Rating") >= 8) & (col("Rating") < 9), "8-9")
                                                   .when((col("Rating") >= 9) & (col("Rating") <= 10), "9-10"))

genre_counts = movies_with_rating.groupBy("RatingRange").count().orderBy(col("RatingRange"))

genre_counts.show()

data_for_plot = genre_counts.collect()

ratings = [row["RatingRange"] for row in data_for_plot]
counts = [row["count"] for row in data_for_plot]

plt.figure(figsize=(10, 6))
plt.bar(ratings, counts, color="skyblue")
plt.xlabel("Rating")
plt.ylabel("Count")
plt.title("Number of movies by the rating")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
