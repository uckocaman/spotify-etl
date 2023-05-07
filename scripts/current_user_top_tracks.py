import spotipy
import pandas as pd
from google.cloud import bigquery
import os
import logging
from mail import send_email

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s :: %(levelname)s :: %(message)s', filename='logs/current_user_top_tracks.log')

logging.info('The job of getting the top tracks started.')

sp = spotipy.Spotify(
    auth_manager=spotipy.oauth2.SpotifyOAuth(client_id = "", 
    client_secret = "", 
    redirect_uri = "http://localhost:7777/callback", 
    scope="user-top-read")
)

def check_if_valid_data(df: pd.DataFrame, key1,key2) -> bool:
    # Check if dataframe is empty
    if df.empty:
        logging.info("No songs downloaded. Finishing execution")
        return False

    # Primary Key Check
    if not df.set_index([key1, key2]).index.is_unique:
        logging.exception("Primary Key check is violated")
        raise

    # Check for nulls
    if df.isnull().values.any():
        logging.exception("Null values found")
        raise

    return True

def get_top_tracks():
    ranges = ['short_term', 'medium_term', 'long_term']

    time_range = []
    track_id = []
    track_name = []
    track_duration = []
    explicit = []
    track_url = []
    is_local = []
    popularity = []
    type = []
    track_number = []
    album_type = []
    album_album_type = []
    album_id = []
    album_name = []
    album_release_date = []
    album_total_tracks = []
    artist_id = []
    artist_name = []

    for range in ranges:
        top_tracks = sp.current_user_top_tracks(limit=50, offset=0, time_range=range)

        for track in top_tracks["items"]:
            time_range.append(range)
            track_id.append(track["id"])
            track_name.append(track["name"])
            track_duration.append(track["duration_ms"])
            explicit.append(track["explicit"])
            track_url.append(track["external_urls"]["spotify"])
            is_local.append(track["is_local"])
            popularity.append(track["popularity"])
            type.append(track["type"])
            track_number.append(track["track_number"])
            album_type.append(track["album"]["type"])
            album_album_type.append(track["album"]["album_type"])
            album_id.append(track["album"]["id"])
            album_name.append(track["album"]["name"])
            album_release_date.append(track["album"]["release_date"])
            album_total_tracks.append(track["album"]["total_tracks"])
            artist_id.append(track["artists"][0]["id"])
            artist_name.append(track["artists"][0]["name"])

    tracks_dict = {
        "time_range" : time_range,
        "track_id" : track_id,
        "track_name": track_name,
        "track_duration" : track_duration,
        "explicit" : explicit,
        "track_url" : track_url,
        "is_local" : is_local,
        "popularity" : popularity,
        "type" : type,
        "track_number" : track_number,
        "album_type" : album_type,
        "album_album_type" : album_album_type,
        "album_id" : album_id,
        "album_name" : album_name,
        "album_release_date" : album_release_date,
        "album_total_tracks" : album_total_tracks,
        "artist_id" : artist_id,
        "artist_name" : artist_name,
    }
    return pd.DataFrame(tracks_dict, columns = list(tracks_dict.keys()))

def load2bq(data,table_id):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=""

    project_id = ''
    dataset_id = ''
    table_id = table_id

    client = bigquery.Client(project = project_id)
    dataset = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition = 'WRITE_TRUNCATE'
    )

    try:
        client.load_table_from_dataframe(data, table, job_config=job_config)
        logging.info(f"Loaded {len(data.index)} rows to {table}")
        send_email(
            subject='The Top Tracks Job Successfully Worked.',
            body=f"Loaded {len(data.index)} rows to {table}",
        )
    except:
        logging.exception("Something went wrong while loading data to BigQuery")
        send_email(subject = 'The Top Tracks Job Got Error', body="Something went wrong while loading data to BigQuery")

        raise

def main():
    try:
        top_tracks = get_top_tracks()    
        logging.info("Data successfully extracted from API, proceeding to validation stage")
    except:
        logging.exception("Something went wrong while extracting data from API")
        raise

    if check_if_valid_data(top_tracks,'time_range','track_id'):
        logging.info("Data valid for top tracks table, proceeding to load stage")  
        load2bq(top_tracks,'my_top_tracks')
     
main()