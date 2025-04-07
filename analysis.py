from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, expr, desc, hour, lit
from datetime import datetime, timedelta

# Initialize Spark session
spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
listening_logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Task 1: Find each user’s favorite genre
user_genre_counts = listening_logs.join(songs_metadata, "song_id").groupBy("user_id", "genre").agg(count("song_id").alias("play_count"))
favorite_genres = user_genre_counts.groupBy("user_id").agg(expr("first(genre) as favorite_genre"))
favorite_genres.write.csv("output/user_favorite_genres", header=True)

# Task 2: Calculate the average listen time per song
avg_listen_time = listening_logs.groupBy("song_id").agg(avg("duration_sec").alias("avg_duration"))
avg_listen_time.write.csv("output/avg_listen_time_per_song", header=True)

# Task 3: List top 10 most played songs this week
one_week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
top_songs = listening_logs.filter(col("timestamp") >= one_week_ago).groupBy("song_id").agg(count("song_id").alias("play_count")).orderBy(desc("play_count")).limit(10)
top_songs.write.csv("output/top_songs_this_week", header=True)

# Task 4: Recommend “Happy” songs to users who mostly listen to “Sad” songs
user_mood_counts = listening_logs.join(songs_metadata, "song_id").groupBy("user_id", "mood").agg(count("song_id").alias("play_count"))
sad_users = user_mood_counts.filter(col("mood") == "Sad").select("user_id").distinct()
happy_songs = songs_metadata.filter(col("mood") == "Happy").select("song_id", "title", "artist")
recommendations = sad_users.crossJoin(happy_songs.limit(3))
recommendations.write.csv("output/happy_recommendations", header=True)

# Task 5: Compute the genre loyalty score
user_total_plays = listening_logs.groupBy("user_id").agg(count("song_id").alias("total_plays"))
genre_loyalty = user_genre_counts.join(user_total_plays, "user_id").withColumn("loyalty_score", col("play_count") / col("total_plays")).filter(col("loyalty_score") > 0.5)
genre_loyalty.write.csv("output/genre_loyalty_scores", header=True)

# Task 6: Identify users who listen between 12 AM and 5 AM
night_owls = listening_logs.withColumn("hour", hour(col("timestamp"))).filter((col("hour") >= 0) & (col("hour") < 5)).select("user_id").distinct()
night_owls.write.csv("output/night_owl_users", header=True)

# Stop Spark session
spark.stop()
