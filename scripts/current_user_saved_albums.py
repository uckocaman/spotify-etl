# Description: This script gets the albums and tracks data from the current user's saved albums on Spotify API.
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

logging.info("The job of getting the albums and tracks data started.")

sp = connect2spotify("user-library-read")


def get_albums() -> pd.DataFrame:
    saved_albums = sp.current_user_saved_albums(limit=50, offset=1)
    albums = []

    for album in saved_albums["items"]:
        album_row = {
            "album_id": album["album"]["id"],
            "album_name": album["album"]["name"],
            "album_label": album["album"]["label"],
            "album_popularity": album["album"]["popularity"],
            "album_release_date": album["album"]["release_date"],
            "album_total_tracks": album["album"]["total_tracks"],
            "album_url": album["album"]["external_urls"]["spotify"],
            "album_type": album["album"]["type"],
            "artist_id": album["album"]["artists"][0]["id"],
            "artist_name": album["album"]["artists"][0]["name"],
            "aded_at": album["added_at"],
        }
        albums.append(album_row)

    return pd.DataFrame(albums)


def get_albums_tracks(album_id_list: list) -> pd.DataFrame:
    album_tracks = []

    for i in album_id_list:
        albums_tracks = sp.album_tracks(i, limit=50, offset=1)
        for item in albums_tracks["items"]:
            album_track_row = {
                "album_id": i,
                "track_id": item["id"],
                "track_name": item["name"],
                "item_type": item["type"],
                "track_duration": item["duration_ms"],
                "explicit": item["explicit"],
                "is_local": item["is_local"],
                "track_number": item["track_number"],
                "artist_id": item["artists"][0]["id"],
                "artist_name": item["artists"][0]["name"],
            }
            album_tracks.append(album_track_row)
    return pd.DataFrame(album_tracks)


def main():
    try:
        albums = get_albums()
        album_ids = albums.album_id.tolist()
        logging.info(
            "Albums data successfully extracted from API, proceeding to validation stage"
        )
    except:
        logging.exception("Something went wrong while extracting albums data from API")
        raise

    if check_if_valid_data(albums, "album_id"):
        logging.info("Data valid for my albums table, proceeding to load stage")
        load2bq(albums, "my_albums")

        try:
            album_tracks = get_albums_tracks(album_ids)
            logging.info(
                "Album tracks data successfully extracted from API, proceeding to validation stage"
            )
        except:
            logging.exception(
                "Something went wrong while extracting album tracks data from API"
            )
            raise

        if check_if_valid_data(album_tracks):
            logging.info("Data valid for album tracks table, proceeding to Load stage")
            load2bq(album_tracks, "album_tracks")


main()
