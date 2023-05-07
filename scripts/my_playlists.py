import spotipy
import pandas as pd
from google.cloud import bigquery
import os
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s :: %(levelname)s :: %(message)s', filename='logs/my_playlists.log')

logging.info('The job of getting the playlists started.')

def connection():
    sp = spotipy.Spotify(
        auth_manager=spotipy.oauth2.SpotifyOAuth(client_id = "", 
        client_secret = "", 
        redirect_uri = "http://localhost:7777/callback", 
        scope="user-library-read")
    )
    my_info = sp.me()
    return sp, my_info["id"]

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

def getPlaylists(sp, user_id):
    playlists = sp.user_playlists(user_id)
    playlist_id = []
    playlist_name = []
    playlist_url = []
    playlist_owner = []
    playlist_owner_url = []
    playlist_owner_id = []
    playlist_owner_type = []
    is_public = []
    total_track = []
    playlist_type = []

    while playlists:
        for playlist in playlists["items"]:
            playlist_id.append(playlist["id"])
            playlist_name.append(playlist["name"])
            playlist_url.append(playlist['external_urls']['spotify'])
            playlist_owner.append(playlist['owner']['display_name'])
            playlist_owner_url.append(playlist['owner']['external_urls']['spotify'])
            playlist_owner_id.append(playlist['owner']['id'])
            playlist_owner_type.append(playlist['owner']['type'])
            is_public.append(playlist['public'])
            total_track.append(playlist['tracks']['total'])
            playlist_type.append(playlist['type'])

        playlists = sp.next(playlists) if playlists["next"] else None
    playlists_dict = {
        "playlist_id" : playlist_id,
        "playlist_name": playlist_name,
        "playlist_url" : playlist_url,
        "playlist_owner_id" : playlist_owner_id,
        "playlist_owner" : playlist_owner,
        "playlist_owner_url" : playlist_owner_url,
        "playlist_owner_type" : playlist_owner_type,
        "is_public" : is_public,
        "total_track" : total_track,
        "playlist_type" : playlist_type,
    }

    playlists_df = pd.DataFrame(playlists_dict, columns = ["playlist_id","playlist_name", "playlist_url","playlist_owner_id","playlist_owner","playlist_owner_url","playlist_owner_type",
                                                            "is_public","total_track","playlist_type"])

    return playlists_dict, playlists_df, playlist_id


def get_playlist_tracks(username, playlist_id_list):
    playlist_id = []
    track_id = []
    track_name = []
    artist_id = []
    artist_name = []
    artist_type = []
    album_id = []
    album_name = []
    album_type = []
    album_release_date = []
    album_total_tracks = []
    track_type = []
    duraiton = []
    added_at = []
    added_by = []
    is_explicit = []

    for i in playlist_id_list:
        results = sp.user_playlist_tracks(username, i)
        playlist_items = results["items"]

        for item in playlist_items:
            playlist_id.append(i)
            track_id.append(item["track"]["id"])
            track_name.append(item["track"]["name"])
            artist_id.append(item["track"]["artists"][0]["id"])
            artist_name.append(item["track"]["artists"][0]["name"])
            artist_type.append(item["track"]["artists"][0]["type"])
            album_id.append(item["track"]["album"]["id"])
            album_name.append(item["track"]["album"]["name"])
            album_type.append(item["track"]["album"]["album_type"])
            album_release_date.append(item["track"]["album"]["release_date"])
            album_total_tracks.append(item["track"]["album"]["total_tracks"])
            track_type.append(item["track"]["type"])
            duraiton.append(item["track"]["duration_ms"])
            added_at.append(item["added_at"])
            added_by.append(item["added_by"]["id"])
            is_explicit.append(item["track"]["explicit"])

        playlist_tracks_dict = {
            "playlist_id" : playlist_id,
            "track_id": track_id,
            "track_name" : track_name,
            "artist_id" : artist_id,
            "artist_name" : artist_name,
            "artist_type" : artist_type,
            "album_id" : album_id,
            "album_name" : album_name,
            "album_type" : album_type,
            "album_release_date" : album_release_date,
            "album_total_tracks" : album_total_tracks,
            "track_type" : track_type,
            "duraiton" : duraiton,
            "added_at" : added_at,
            "added_by" :added_by,
            "is_explicit" : is_explicit
        }

        while results["next"]:
            results = sp.next(results)
            playlist_items.append(results["items"])

    return pd.DataFrame(
        playlist_tracks_dict,
        columns=[
            "playlist_id",
            "track_id",
            "track_name",
            "artist_id",
            "artist_name",
            "artist_type",
            "album_id",
            "album_name",
            "album_type",
            "album_release_date",
            "album_total_tracks",
            "track_type",
            "duraiton",
            "added_at",
            "added_by",
            "is_explicit",
        ],
    )

def load2bq(data,project_id,dataset_id,table_id,write_disposition):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=""

    client = bigquery.Client(project = project_id)
    dataset = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition = write_disposition
    )
    try:
        client.load_table_from_dataframe(data, table, job_config=job_config)
        logging.info(f"Loaded {len(data.index)} rows to {table}")
    except:
        logging.exception("Something went wrong while loading data to BigQuery")
        raise

if __name__ == "__main__":
    sp, id = connection()
    try:
        playlists_dict, playlists_df, playlist_id_list = getPlaylists(sp,id)
        logging.info("Playlists data successfully extracted from API, proceeding to validation stage")
    except:
        logging.exception("Something went wrong while extracting playlists data from API")
        raise
        
    if check_if_valid_data(playlists_df,"playlist_id"):
        logging.info("Data valid, proceeding to load stage")
        load2bq(playlists_df, project_id = 'telegrambot-295900', dataset_id = 'dwh', table_id = 'my_playlists', write_disposition = 'WRITE_TRUNCATE')
        
        playlist_tracks_df = get_playlist_tracks(id, playlist_id_list)
        
        if check_if_valid_data(playlist_tracks_df):
            logging.info("Data valid, proceeding to load stage")
            load2bq(playlist_tracks_df, project_id = 'telegrambot-295900', dataset_id = 'dwh', table_id = 'my_playlists_tracks', write_disposition = 'WRITE_TRUNCATE')