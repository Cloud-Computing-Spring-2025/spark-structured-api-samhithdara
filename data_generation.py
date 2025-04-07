import csv
from datetime import datetime, timedelta
import random

# Generate listening_logs.csv
def generate_listening_logs(filename="listening_logs.csv", num_records=1000):
    users = [f"U{i}" for i in range(1, 51)]  # 50 users
    songs = [f"S{i}" for i in range(1, 101)]  # 100 songs
    
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["user_id", "song_id", "timestamp", "duration_sec"])
        
        for _ in range(num_records):
            user_id = random.choice(users)
            song_id = random.choice(songs)
            
            # Generate a random date in the past 30 days
            random_days_ago = random.randint(0, 30)
            random_time_of_day = random.randint(0, 86399)  # Number of seconds in a day

            random_datetime = datetime.now() - timedelta(days=random_days_ago, seconds=random_time_of_day)
            timestamp = random_datetime.strftime('%Y-%m-%d %H:%M:%S')

            duration_sec = random.randint(30, 300)  # Duration between 30 sec and 5 min
            writer.writerow([user_id, song_id, timestamp, duration_sec])


# Generate songs_metadata.csv
def generate_songs_metadata(filename="songs_metadata.csv"): 
    songs = [f"S{i}" for i in range(1, 101)]
    titles = [f"Song {i}" for i in range(1, 101)]
    artists = [f"Artist {random.randint(1, 20)}" for _ in range(100)]  # 20 unique artists
    genres = ["Pop", "Rock", "Jazz", "Hip-Hop", "Classical"]
    moods = ["Happy", "Sad", "Energetic", "Chill"]
    
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["song_id", "title", "artist", "genre", "mood"])
        
        for i in range(100):
            writer.writerow([songs[i], titles[i], artists[i], random.choice(genres), random.choice(moods)])

# Generate datasets
generate_listening_logs()
generate_songs_metadata()
