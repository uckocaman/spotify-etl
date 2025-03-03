# Description: This script gets the genres from Spotify API and loads it to BigQuery table.
import pandas as pd
import os
import logging
from load2bq import load2bq
from dotenv import load_dotenv
from validations import check_if_valid_data
from connect_to_spotify import connect2spotify

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s :: %(levelname)s :: %(message)s",
    filename=f"logs/{os.path.basename(__file__).split('.')[0]}.log",
)
logging.info("The job of getting the playlists started.")

sp = connect2spotify("user-library-read")

def get_genres() -> pd.DataFrame:
    genres = sp.recommendation_genre_seeds()
    return pd.DataFrame(genres, columns=list(genres.keys()))


def main():
    genres = get_genres()

    if check_if_valid_data(genres):
        print("Data valid for genres table, proceed to Load stage")
        load2bq(genres, "genres")


main()
