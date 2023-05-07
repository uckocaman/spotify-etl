import spotipy
import pandas as pd
from google.cloud import bigquery
import os
import os

import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s :: %(levelname)s :: %(message)s', filename='logs/current_user_saved_tracks.log')
logging.info('The job of getting the saved tracks started.')

sp = spotipy.Spotify(
    auth_manager=spotipy.oauth2.SpotifyOAuth(client_id = "", 
    client_secret = "", 
    redirect_uri = "http://localhost:7777/callback", 
    scope="user-library-read")
)

def check_if_valid_data(df: pd.DataFrame, primary_key = 'None') -> bool:
    # Check if dataframe is empty
    if df.empty:
        logging.info("No songs downloaded. Finishing execution")
        return False

    # Primary Key Check
    if primary_key != 'None' and not pd.Series(df[primary_key]).is_unique:
        logging.exception("Primary Key check is violated")
        raise

    # Check for nulls
    if df.isnull().values.any():
        logging.exception("Null values found")
        raise

    return True

def get_saved_tracks(offset = 0):
    saved_tracks = sp.current_user_saved_tracks(limit=50, offset=offset)

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
    album_id = []
    album_name = []
    album_release_date = []
    album_total_tracks = []
    artist_id = []
    artist_name = []
    added_at= []

    for track in saved_tracks["items"]:
        track_id.append(track["track"]["id"])
        track_name.append(track["track"]["name"])
        track_duration.append(track["track"]["duration_ms"])
        explicit.append(track["track"]["explicit"])
        track_url.append(track["track"]["external_urls"]["spotify"])
        is_local.append(track["track"]["is_local"])
        popularity.append(track["track"]["popularity"])
        type.append(track["track"]["type"])
        track_number.append(track["track"]["track_number"])
        album_type.append(track["track"]["album"]["type"])
        album_id.append(track["track"]["album"]["id"])
        album_name.append(track["track"]["album"]["name"])
        album_release_date.append(track["track"]["album"]["release_date"])
        album_total_tracks.append(track["track"]["album"]["total_tracks"])
        artist_id.append(track["track"]["artists"][0]["id"])
        artist_name.append(track["track"]["artists"][0]["name"])
        added_at.append(track["added_at"])

    tracks_dict = {
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
        "album_id" : album_id,
        "album_name" : album_name,
        "album_release_date" : album_release_date,
        "album_total_tracks" : album_total_tracks,
        "artist_id" : artist_id,
        "artist_name" : artist_name,
        "aded_at" : added_at
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
    except:
        logging.exception("Something went wrong while loading data to BigQuery")
        raise

def main():
    try:
        saved_tracks = get_saved_tracks()    

        logging.info("Data successfully extracted from API, proceeding to validation stage")
    except:
        logging.exception("Something went wrong while extracting data from API")
        raise

#  TODO -> Update it to request the api until you get all the data using the loop

    if len(saved_tracks.index) < 50:
        logging.info("Data successfully extracted from API, proceeding to validation stage")
        if check_if_valid_data(saved_tracks,'track_id'):
            logging.info("Data valid for tracks table, proceed to Load stage")            
            load2bq(saved_tracks,'saved_tracks')
    else:
        logging.info("The number of saved show is greater than 50. The offset increased.")
        try:
            saved_tracks_second_offset = get_saved_tracks(offset=51)
            full = pd.concat([saved_tracks, saved_tracks_second_offset])
            logging.info("All data successfully extracted from API, proceeding to validation stage")
        except:
            logging.exception("Something went wrong while extracting data from API")
            raise

        if check_if_valid_data(full,'track_id'):
            logging.info("Data valid for tracks table, proceed to Load stage")            
            load2bq(full,'saved_tracks')
     
main()