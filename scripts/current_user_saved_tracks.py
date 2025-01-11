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
logging.info("The job of getting the saved tracks started.")

sp = connect2spotify("user-library-read")


def get_saved_tracks(offset: int = 0):
    saved_tracks = sp.current_user_saved_tracks(limit=50, offset=offset)
    total_track = saved_tracks["total"]
    saved_tracks_list = []

    for track in saved_tracks["items"]:
        tracks_row = {
            "track_id": track["track"]["id"],
            "track_name": track["track"]["name"],
            "track_duration": track["track"]["duration_ms"],
            "explicit": track["track"]["explicit"],
            "track_url": track["track"]["external_urls"]["spotify"],
            "is_local": track["track"]["is_local"],
            "popularity": track["track"]["popularity"],
            "type": track["track"]["type"],
            "track_number": track["track"]["track_number"],
            "album_type": track["track"]["album"]["type"],
            "album_id": track["track"]["album"]["id"],
            "album_name": track["track"]["album"]["name"],
            "album_release_date": track["track"]["album"]["release_date"],
            "album_total_tracks": track["track"]["album"]["total_tracks"],
            "artist_id": track["track"]["artists"][0]["id"],
            "artist_name": track["track"]["artists"][0]["name"],
            "aded_at": track["added_at"],
        }
        saved_tracks_list.append(tracks_row)

    return pd.DataFrame(saved_tracks_list), total_track


def main():
    try:
        saved_tracks, total_track = get_saved_tracks()

        logging.info(
            "Data successfully extracted from API, proceeding to validation stage"
        )
    except:
        logging.exception("Something went wrong while extracting data from API")
        raise

    while len(saved_tracks) % 50 == 0 and len(saved_tracks) != total_track:
        logging.info(
            f"The number of saved tracks is greater than {len(saved_tracks)}. The offset increased to {len(saved_tracks)+1}"
        )

        try:
            saved_tracks_new_offset, total_track = get_saved_tracks(
                offset=len(saved_tracks) + 1
            )
            saved_tracks = pd.concat([saved_tracks, saved_tracks_new_offset])
            logging.info(
                "All data successfully extracted from API, proceeding to validation stage"
            )
        except:
            logging.exception("Something went wrong while extracting data from API")
            raise

    if check_if_valid_data(saved_tracks, "track_id"):
        logging.info("Data valid for tracks table, proceed to Load stage")
        load2bq(saved_tracks, "saved_tracks")


main()
