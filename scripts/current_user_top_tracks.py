# Description: This script gets the saved tracks of the user and loads them into BigQuery.
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
logging.info("The job of getting the top tracks started.")

sp = connect2spotify("user-library-read")


def get_top_tracks() -> pd.DataFrame:
    ranges = ["short_term", "medium_term", "long_term"]
    top_tracks_list = []

    for range in ranges:
        top_tracks = sp.current_user_top_tracks(limit=50, offset=0, time_range=range)
        for track in top_tracks["items"]:
            tracks_row = {
                "time_range": range,
                "track_id": track["id"],
                "track_name": track["name"],
                "track_duration": track["duration_ms"],
                "explicit": track["explicit"],
                "track_url": track["external_urls"]["spotify"],
                "is_local": track["is_local"],
                "popularity": track["popularity"],
                "type": track["type"],
                "track_number": track["track_number"],
                "album_type": track["album"]["type"],
                "album_album_type": track["album"]["album_type"],
                "album_id": track["album"]["id"],
                "album_name": track["album"]["name"],
                "album_release_date": track["album"]["release_date"],
                "album_total_tracks": track["album"]["total_tracks"],
                "artist_id": track["artists"][0]["id"],
                "artist_name": track["artists"][0]["name"],
            }
            top_tracks_list.append(tracks_row)

    return pd.DataFrame(top_tracks_list)


def main():
    try:
        top_tracks = get_top_tracks()
        logging.info(
            "Data successfully extracted from API, proceeding to validation stage"
        )
    except:
        logging.exception("Something went wrong while extracting data from API")
        raise

    if check_if_valid_data(top_tracks, "time_range", "track_id"):
        logging.info("Data valid for top tracks table, proceeding to load stage")
        load2bq(top_tracks, "my_top_tracks")


main()
