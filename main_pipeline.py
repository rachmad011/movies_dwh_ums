import pandas as pd
import sqlalchemy
import os
import re
import json
import numpy as np
import math
from dotenv import load_dotenv

load_dotenv()

# Gunakan ini jika ingin mengambil data dari database

mysql_host = os.getenv("mysql_host")
mysql_username = os.getenv("mysql_username")
mysql_password = os.getenv("mysql_password")
mysql_port = os.getenv("mysql_port")
mysql_db_name_movie = os.getenv("mysql_db_name_movie")
mysql_db_name_rating = os.getenv("mysql_db_name_rating")

engine_movie = sqlalchemy.create_engine(
    f'mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db_name_movie}',
    echo=False)

engine_rating = sqlalchemy.create_engine(
    f'mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db_name_rating}',
    echo=False)

# Fungsi untuk mentransformasi genre menjadi format json array
def transform_genre(genre):
    if genre == "" or genre is None:
        return None
    
    if '|' not in genre:
        return json.dumps([genre])
    
    return json.dumps(genre.split("|"))

# Fungsi untuk mendapatkan tahun film dari judul film / title
def get_year(title):
    if ')' not in title[-6:]:
        return 0
    
    # rm_brackets = re.search(f"\(.*?\)", title[-6:])
    rm_brackets = re.findall(f"\d", title[-6:])
    
    if len(rm_brackets) != 4:
        return None
    
    return int(''.join(rm_brackets))

# Fungsi untuk membersihkan judul film / title
def clean_title(title):
    if ')' not in title and '(' not in title:
        return title.strip()
    
    rm_brackets = re.search(f"\(.*?\)", title)
    title = title.replace(rm_brackets[0], "")
    
    return title.strip()

# Funsi untuk melakukan Insert data baru dan memperbaharui data yang sudah ada ke dalam database MySQL (harusnya disimpan ke Data Warehouse)
def upsert_data(movie_id, title, genre, rating_avg, year):
    
    if year == 0:
        year = "NULL"
    
    query_upsert = f"""INSERT INTO `movie_rating` VALUE ({movie_id}, "{title}", '{genre}', 
                    {rating_avg}, {year}) ON DUPLICATE KEY UPDATE movieId={movie_id}, title="{title}", genre='{genre}', 
                    rating_avg={rating_avg}, year={year}"""
    
    try:
        with engine_movie.connect() as conn_movie:
            result = conn_movie.execute(sqlalchemy.text(query_upsert))
        
    except sqlalchemy.exc.SQLAlchemyError as e:
        error = str(e)
        print(error)


if __name__ == '__main__':
    print("Data Pipeline Starting!\n")
    
    sql_movie = """SELECT * FROM movie"""
    sql_rating = """SELECT * FROM rating"""

    # Hilangkan komentar pada kode di bawah ini untuk mengambil data menggunakan MySQL
    # data_movie = pd.read_sql(sql_movie, engine_movie)
    # data_rating = pd.read_sql(sql_rating, engine_rating)

    # Kali ini menggunakan data dalam format .csv agar lebih cepat diproses
    data_movie = pd.read_csv("movie.csv")
    data_rating = pd.read_csv("rating.csv")
    
    # Berikut ini adalah kode untuk mendapatkan rata-rata rating per movieId
    avg_movie_rating = data_rating.groupby('movieId', as_index=False)['rating'].mean()
    avg_movie_rating = avg_movie_rating.rename(columns = {'rating':'rating_avg', 'genres':'genre'}, inplace = False)
    avg_movie_rating['rating_avg'] = avg_movie_rating['rating_avg'].round(decimals=1)
    
    # Mengubah nama kolom dari 'genres' menjadi 'genre'
    data_movie = data_movie.rename(columns = {'genres':'genre'}, inplace = False)

    # Proses transformasi judul film, genre, dan juga tahun film
    movie_joined = pd.merge(data_movie, avg_movie_rating, how="left", on="movieId")
    movie_joined = movie_joined.fillna(0)
    movie_joined['title'] = movie_joined['title'].apply(lambda x: x.strip())
    movie_joined['year'] = movie_joined['title'].apply(lambda x: get_year(x))
    movie_joined['title'] = movie_joined['title'].apply(lambda x: clean_title(x))
    movie_joined['genre'] = movie_joined['genre'].apply(lambda x: x.strip())
    movie_joined['genre'] = movie_joined['genre'].apply(lambda x: x.replace("(no genres listed)", ""))
    movie_joined['genre'] = movie_joined['genre'].apply(lambda x: transform_genre(x))
    movie_joined['year'] = movie_joined['year'].fillna(0)
    movie_joined = movie_joined.astype({'year':'Int64'})
    
    for index, row in movie_joined.iterrows():
        upsert_data(row['movieId'], row['title'], row['genre'], row['rating_avg'], row['year'])
    
    print("\nAll Tasks is Done!")