from load_data import load_data
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, row_number, expr
from pyspark.sql.window import Window

data = load_data()

df_name_basics = data['name_basics']
df_title_akas = data['title_akas']
df_title_basics = data['title_basics']
df_title_crew = data['title_crew']
df_title_ratings = data['title_ratings']

window_spec = Window.partitionBy("tconst")


def run_maks_queries(dfs):

        #Запит 1
    print(f'-' * 21 + 'Всі фільми з режисерами без Null значень' + '-' * 21)

    df_title_crew_temp = df_title_crew.filter(
        (df_title_crew.directors.isNotNull()) &
        (F.size(F.split(df_title_crew.directors, ",")) <= 1)
    )
    df_title_basics_temp = df_title_basics.filter(df_title_basics.tconst.isNotNull())

    result1 = df_title_basics_temp.join(df_title_crew_temp, "tconst").select("tconst", "primaryTitle", "directors")
    result1 = result1.join(df_name_basics, result1.directors == df_name_basics.nconst, "left").select("primaryTitle", "directors", "primaryName")
    result1 = result1.withColumnRenamed("primaryTitle", "filmName").withColumnRenamed("directors", "director_ids").withColumnRenamed("primaryName", "Names")

    result1.show(20)
    print(f"Загальна кількість рядків: {result1.count()}")

    #Запит 2
    print(f'-' * 21 + 'Фільми, режисером яких є Benedict' + '-' * 21)

    df_title_crew_temp = df_title_crew.filter(df_title_crew.directors.isNotNull())
    df_name_basics_temp = df_name_basics.filter(df_name_basics.primaryName.contains("Benedict"))

    result2 = df_title_crew_temp.join(df_name_basics_temp, df_title_crew_temp.directors == df_name_basics_temp.nconst)
    result2 = result2.join(df_title_basics, result2.tconst == df_title_basics.tconst).select("primaryTitle", "directors", "primaryName")
    result2 = result2.withColumnRenamed("primaryTitle", "filmName").withColumnRenamed("directors", "director_ids").withColumnRenamed("primaryName", "Names")
    result2 = result2.dropDuplicates(["director_ids"]) #без повторів

    result2.show(20)
    print(f"Загальна кількість рядків: {result2.count()}")



    #Запит 3
    print(f'-' * 21 + 'Список людей, які прожили рівно 50 років і вмерли' + '-' * 21)

    result3 = df_name_basics.filter(col("birthYear").isNotNull() & col("deathYear").isNotNull())

    result3 = result3.withColumn("ageAtDeath", expr("deathYear - birthYear"))
    result3 = result3.filter(col("ageAtDeath") == 50)

    result3 = result3.select(
        col("primaryName").alias("name"),
        col("birthYear").alias("birth"),
        col("deathYear").alias("Death"),
        col("ageAtDeath")
    )

    result3.show(20)
    print(f"Загальна кількість рядків: {result3.count()}")

    #Запит 4
    print(f'-' * 21 + 'Різні поєднання груп професій, серед яких є Music_Department' + '-' * 21)

    result4 = df_name_basics.filter(df_name_basics.primaryProfession.contains("music_dep"))

    result4 = result4.groupBy("primaryProfession").agg(F.collect_list("primaryName").alias("names"))
    result4 = result4.withColumnRenamed("primaryProfession", "containsMusic_Department").withColumnRenamed("names", "fullName")

    result4.show(truncate=False)
    print(f"Загальна кількість рядків: {result4.count()}")

    #Запит 5
    print(f'-' * 21 + 'Групування фільмів по регіонам, з фільтром, щоб параметри були не Null, і ті, назви яких не були змінені (тобто isoriginaltitle=1)' + '-' * 21)

    result5 = df_title_akas.filter(
        F.col("title").isNotNull() &
        F.col("region").isNotNull() &
        F.col("language").isNotNull()
    )

    result5 = result5.groupBy("language").agg(F.count("*").alias("original_title_count")).orderBy(F.desc("original_title_count"))

    result5.show(20)
    print(f"Загальна кількість рядків: {result5.count()}")

    #Запит 6
    print(f'-' * 21 + 'Фільми, де numvotes>10000 і які вийшли у 2024' + '-' * 21)

    result6 = df_title_basics.filter(df_title_basics.startYear == "2024")
    result6 = result6.join(df_title_ratings, "tconst")

    result6 = result6.withColumn("over_10k_votes",when(col("numVotes") > 10000, 1).otherwise(0))
    result6 = result6.filter(col("over_10k_votes") == 1).select("primaryTitle", "startYear", "numVotes")

    result6 = result6.sort(col("numVotes").desc())

    result6.show(20)
    print(f"Загальна кількість рядків: {result6.count()}")

