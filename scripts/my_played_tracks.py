import spotipy
import datetime
import pandas as pd
from google.cloud import bigquery
import pytz
import os
from dateutil import parser
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s :: %(levelname)s :: %(message)s",
    filename="logs/my_played_tracks.log",
)

logging.info("The job of getting the played tracks started.")

sp = spotipy.Spotify(
    auth_manager=spotipy.oauth2.SpotifyOAuth(
        client_id="",
        client_secret="",
        redirect_uri="http://localhost:7777/callback",
        scope="user-read-recently-played",
    )
)


def check_if_valid_data(df: pd.DataFrame, unix_timestamp) -> bool:
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

        if diff > 0:
            logging.exception(
                "At least one of the returned songs does not have a last one hour's timestamp"
            )
            raise
    return True


def extract(time_interval):
    results = sp.current_user_recently_played(limit=50, after=time_interval)

    song_name = []
    song_url = []
    song_id = []
    song_release_date = []
    album_name = []
    album_url = []
    duration_ms = []
    artist_name = []
    artist_profile_url = []
    artist_id = []
    played_at = []
    timestamp_ = []

    for song in results["items"]:
        song_name.append(song["track"]["name"])
        song_url.append(song["track"]["external_urls"]["spotify"])
        song_id.append(song["track"]["id"])
        song_release_date.append(song["track"]["album"]["release_date"])
        album_name.append(song["track"]["album"]["name"])
        album_url.append(song["track"]["album"]["external_urls"]["spotify"])
        duration_ms.append(song["track"]["duration_ms"])
        artist_name.append(song["track"]["album"]["artists"][0]["name"])
        artist_profile_url.append(
            song["track"]["album"]["artists"][0]["external_urls"]["spotify"]
        )
        artist_id.append(song["track"]["album"]["artists"][0]["id"])
        played_at.append(song["played_at"])
        timestamp_.append(song["played_at"][:10])

    song_dict = {
        "song_name": song_name,
        "song_url": song_url,
        "song_id": song_id,
        "song_release_date": song_release_date,
        "album_name": album_name,
        "album_url": album_url,
        "duration_ms": duration_ms,
        "artist_name": artist_name,
        "artist_profile_url": artist_profile_url,
        "artist_id": artist_id,
        "played_at": played_at,
        "timestamp": timestamp_,
    }

    song_df = pd.DataFrame(
        song_dict,
        columns=[
            "song_name",
            "song_url",
            "song_id",
            "song_release_date",
            "album_name",
            "album_url",
            "duration_ms",
            "artist_name",
            "artist_id",
            "played_at",
            "timestamp_",
        ],
    )
    song_df["timestamp_"].fillna(
        value=pd.to_datetime(datetime.date.today()), inplace=True
    )

    return song_df


def load2bq(data):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""

    project_id = ""
    dataset_id = ""
    table_id = "my_played_tracks"

    client = bigquery.Client(project=project_id)
    dataset = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition="WRITE_APPEND",
    )

    try:
        client.load_table_from_dataframe(data, table, job_config=job_config)
        logging.info(f"Loaded {len(data.index)} rows to {table}")
    except:
        logging.exception("Something went wrong while loading data to BigQuery")
        raise


if __name__ == "__main__":
    current_datetime = datetime.datetime.now()
    an_hour_ago = current_datetime - datetime.timedelta(hours=24)
    unix_timestamp = int(an_hour_ago.timestamp()) * 1000

    try:
        new_data = extract(unix_timestamp)
        logging.info(
            "Data successfully extracted from API, proceeding to validation stage"
        )
    except:
        logging.exception("Something went wrong while extracting data from API")
        raise

    if check_if_valid_data(new_data, current_datetime.strftime("%Y-%m-%d %H:%M:%S")):
        logging.info("Data valid, proceeding to load stage")
        load2bq(new_data)
