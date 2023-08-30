import spotipy
import datetime
import pandas as pd
import pytz
import os
from dateutil import parser
import logging
from load2bq import load2bq

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
        scope="user-library-read",
    )
)


def check_if_valid_data(df: pd.DataFrame, intrerval_hour) -> bool:
    # Check if dataframe is empty
    if df.empty:
        logging.info("No songs downloaded. Finishing execution")
        return False

    # Primary Key Check
    if not pd.Series(df["played_at"]).is_unique:
        logging.exception("Primary Key check is violated")
        raise

    # Check for nulls
    if df.isnull().values.any():
        logging.exception("Null values found")
        raise

    # Check that all timestamps are of last hour's date
    timestamps = df["played_at"].tolist()
    for ts in timestamps:
        ts = parser.parse(ts).astimezone(pytz.timezone("Europe/Istanbul"))
        ts = ts.replace(tzinfo=None)
        diff = int((current_datetime - ts).seconds // 3600)

        if diff > intrerval_hour:
            logging.exception(
                "At least one of the returned songs do not in the interval"
            )
            raise
    return True


def extract(time_interval) -> pd.DataFrame:
    results = sp.current_user_recently_played(limit=50, after=time_interval)
    played_tracks_list = []

    for song in results["items"]:
        song_row = {
            "song_name":song["track"]["name"],
            "song_url":song["track"]["external_urls"]["spotify"],
            "song_id":song["track"]["id"],
            "song_release_date":song["track"]["album"]["release_date"],
            "album_name":song["track"]["album"]["name"],
            "album_url":song["track"]["album"]["external_urls"]["spotify"],
            "duration_ms":song["track"]["duration_ms"],
            "artist_name":song["track"]["album"]["artists"][0]["name"],
            "artist_profile_url":song["track"]["album"]["artists"][0]["external_urls"]["spotify"],
            "artist_id":song["track"]["album"]["artists"][0]["id"],
            "played_at":song["played_at"],
            "timestamp_":song["played_at"][:10]
        }
        played_tracks_list.append(song_row)

    song_df = pd.DataFrame(played_tracks_list)
    song_df["timestamp_"].fillna(value=pd.to_datetime(datetime.date.today()), inplace=True)

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

    if check_if_valid_data(my_played_tracks, intrerval_hour):
        logging.info("Data valid, proceeding to load stage")
        load2bq(my_played_tracks, "my_played_tracks", "WRITE_APPEND")
