import spotipy
import pandas as pd
from google.cloud import bigquery
import os
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s :: %(levelname)s :: %(message)s', filename='logs/current_user_saved_albums.log')

logging.info('The job of getting the saved albums started.')

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

def get_albums():
    albums = sp.current_user_saved_albums(limit=50, offset=1)

    album_id = []
    album_name = []
    album_label= []
    album_popularity= []
    album_release_date = []
    album_total_tracks = []
    album_url = []
    album_type = []
    artist_id = []
    artist_name= []
    added_at= []
   
    for album in albums["items"]:
        album_id.append(album["album"]["id"])
        album_name.append(album["album"]["name"])
        album_label.append(album["album"]["label"])
        album_popularity.append(album["album"]["popularity"])
        album_release_date.append(album["album"]["release_date"])
        album_total_tracks.append(album["album"]["total_tracks"])
        album_url.append(album["album"]["external_urls"]["spotify"])
        album_type.append(album["album"]["type"])
        artist_id.append(album["album"]["artists"][0]["id"])
        artist_name.append(album["album"]["artists"][0]["name"])
        added_at.append(album["added_at"])

    album_dict = {
        "album_id" : album_id,
        "album_name": album_name,
        "album_label" : album_label,
        "album_popularity" : album_popularity,
        "album_release_date" : album_release_date,
        "album_total_tracks" : album_total_tracks,
        "album_url" : album_url,
        "album_type" : album_type,
        "artist_id" : artist_id,
        "artist_name" : artist_name,
        "aded_at" : added_at
    }
    album_df = pd.DataFrame(album_dict, columns = list(album_dict.keys()))
    
    return album_df, album_id

def get_albums_tracks(album_id_list):

    album_id = []
    track_id = []
    track_name = []
    item_type = []
    track_duration = []
    explicit = []
    is_local = []
    track_number = []
    artist_id = []
    artist_name = []

    for i in album_id_list:
        albums_tracks = sp.album_tracks(i,limit=50, offset=1)
        album_items = albums_tracks["items"]

        for item in album_items:
#            print(len(item["artists"]))
            album_id.append(i)
            track_id.append(item["id"])
            track_name.append(item["name"])
            item_type.append(item["type"])
            track_duration.append(item["duration_ms"])
            explicit.append(item["explicit"])
            is_local.append(item["is_local"])
            track_number.append(item["track_number"])
            artist_id.append(item["artists"][0]["id"])
            artist_name.append(item["artists"][0]["name"])

        album_track_dict = {
            "album_id" : album_id,
            "track_id": track_id,
            "track_name" : track_name,
            "item_type" : item_type,
            "track_duration" : track_duration,
            "explicit" : explicit,
            "is_local" : is_local,
            "track_number" : track_number,
            "artist_id" : artist_id,
            "artist_name" : artist_name,
        }
    return pd.DataFrame(album_track_dict, columns = list(album_track_dict.keys()))


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
        albums, album_id = get_albums()    
        logging.info("Data successfully extracted from API, proceeding to validation stage")
    except:
        logging.exception("Something went wrong while extracting data from API")
        raise

    if check_if_valid_data(albums,'album_id'):
        logging.info("Data valid for albums table, proceeding to Load stage")
        load2bq(albums,'my_albums')

        try:
            album_tracks = get_albums_tracks(album_id)
        except:
            logging.exception("Something went wrong while extracting data from API")
            raise
        
        if check_if_valid_data(album_tracks):
            logging.info("Data valid for album tracks table, proceeding to Load stage")
            load2bq(album_tracks,'album_tracks')
main()