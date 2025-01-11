import spotipy
import datetime
import pandas as pd
import os
import logging
from load2bq import load2bq
from dotenv import load_dotenv
from validations import check_if_valid_data, check_if_valid_interval

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s :: %(levelname)s :: %(message)s",
    filename=f"logs/{os.path.basename(__file__).split('.')[0]}.log",
)

logging.info("The job of getting the played tracks started.")

sp = spotipy.Spotify(
    auth_manager=spotipy.oauth2.SpotifyOAuth(
        client_id=os.environ.get("SPOTIFY_CLIENT_ID"),
        client_secret=os.environ.get("SPOTIFY_CLIENT_SECRET"),
        redirect_uri="http://localhost:7777/callback",
        scope="user-read-recently-played",
    )
)


def extract(time_interval) -> pd.DataFrame:
    results = sp.current_user_recently_played(limit=50, after=time_interval)
    played_tracks_list = []

    for song in results["items"]:
        song_row = {
            "song_name": song["track"]["name"],
            "song_url": song["track"]["external_urls"]["spotify"],
            "song_id": song["track"]["id"],
            "song_release_date": song["track"]["album"]["release_date"],
            "album_name": song["track"]["album"]["name"],
            "album_url": song["track"]["album"]["external_urls"]["spotify"],
            "duration_ms": song["track"]["duration_ms"],
            "artist_name": song["track"]["album"]["artists"][0]["name"],
            "artist_profile_url": song["track"]["album"]["artists"][0]["external_urls"][
                "spotify"
            ],
            "artist_id": song["track"]["album"]["artists"][0]["id"],
            "played_at": song["played_at"],
            "timestamp_": song["played_at"][:10],
        }
        played_tracks_list.append(song_row)

    song_df = pd.DataFrame(played_tracks_list)
    song_df["timestamp_"] = song_df["timestamp_"].fillna(
        value=pd.to_datetime(datetime.date.today())
    )

    return song_df


if __name__ == "__main__":
    current_datetime = datetime.datetime.now()
    intrerval_hour = 24
    lower_interval = current_datetime - datetime.timedelta(hours=intrerval_hour)
    unix_timestamp = int(lower_interval.timestamp()) * 1000

    try:
        my_played_tracks = extract(unix_timestamp)
        logging.info(
            "Data successfully extracted from API, proceeding to validation stage"
        )
    except:
        logging.exception("Something went wrong while extracting data from API")
        raise

    timestamps = my_played_tracks["played_at"].tolist()
    if check_if_valid_data(my_played_tracks, "played_at") and check_if_valid_interval(
        timestamps, current_datetime, intrerval_hour
    ):
        logging.info("Data valid, proceeding to load stage")
        load2bq(my_played_tracks, "my_played_tracks", "WRITE_APPEND")
