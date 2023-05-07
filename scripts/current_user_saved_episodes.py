import spotipy
import pandas as pd
from google.cloud import bigquery
import os
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s :: %(levelname)s :: %(message)s', filename='logs/current_user_saved_episodes.log')

logging.info('The job of getting the played tracks started.')

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

def get_saved_episodes(offset = 0):
    saved_episodes = sp.current_user_saved_episodes(limit=50, offset=offset)

    episode_id = []
    episode_name = []
    episode_description = []
    episode_duration= []
    explicit = []
    episode_url = []
    is_externally_hosted = []
    is_playable = []
    language = []
    release_date = []
    type = []
    show_description = []
    show_explicit = []
    show_url = []
    show_id = []
    show_name = []
    show_publisher = []
    shwo_total_episodes = []
    show_is_externally_hosted = []
    show_media_type = []
    added_at= []

    for episode in saved_episodes["items"]:
        episode_id.append(episode["episode"]["id"])
        episode_name.append(episode["episode"]["name"])
        episode_description.append(episode["episode"]["description"])
        episode_duration.append(episode["episode"]["duration_ms"])
        explicit.append(episode["episode"]["explicit"])
        episode_url.append(episode["episode"]["external_urls"]["spotify"])
        is_externally_hosted.append(episode["episode"]["is_externally_hosted"])
        is_playable.append(episode["episode"]["is_playable"])
        language.append(episode["episode"]["language"])
        release_date.append(episode["episode"]["release_date"])
        type.append(episode["episode"]["type"])
        show_description.append(episode["episode"]["show"]["description"])
        show_explicit.append(episode["episode"]["show"]["explicit"])
        show_url.append(episode["episode"]["show"]["external_urls"]["spotify"])
        show_id.append(episode["episode"]["show"]["id"])
        show_name.append(episode["episode"]["show"]["name"])
        show_publisher.append(episode["episode"]["show"]["publisher"])
        shwo_total_episodes.append(episode["episode"]["show"]["total_episodes"])
        show_is_externally_hosted.append(episode["episode"]["show"]["is_externally_hosted"])
        show_media_type.append(episode["episode"]["show"]["media_type"])
        added_at.append(episode["added_at"])

    episodes_dict = {
        "episode_id" : episode_id,
        "episode_name": episode_name,
        "episode_description" : episode_description,
        "episode_duration" : episode_duration,
        "explicit" : explicit,
        "episode_url" : episode_url,
        "is_externally_hosted" : is_externally_hosted,
        "is_playable" : is_playable,
        "language" : language,
        "release_date" : release_date,
        "type" : type,
        "show_description" : show_description,
        "show_explicit" : show_explicit,
        "show_url" : show_url,
        "show_id" : show_id,
        "show_name" : show_name,
        "show_publisher" : show_publisher,
        "shwo_total_episodes" : shwo_total_episodes,
        "show_is_externally_hosted" : show_is_externally_hosted,
        "show_media_type" : show_media_type,
        "aded_at" : added_at
    }
    return pd.DataFrame(episodes_dict, columns = list(episodes_dict.keys()))

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
        saved_episodes = get_saved_episodes() 
        logging.info("Data successfully extracted from API, proceeding to validation stage")
    except:
        logging.exception("Something went wrong while extracting data from API")
        raise

#  TODO -> Update it to request the api until you get all the data using the loop

    if len(saved_episodes.index) == 50:
        logging.info("The number of saved episode is greater than 50. The offset increased.")
        try:
            saved_episodes_second_offset = get_saved_episodes(offset=51)
            full = pd.concat([saved_episodes, saved_episodes_second_offset])
            logging.info("All data successfully extracted from API, proceeding to validation stage")
        except:
            logging.exception("Something went wrong while extracting data from API")
            raise
        
        if check_if_valid_data(full,'episode_id'):
            logging.info("Data valid for albums table, proceed to Load stage")
            load2bq(full,'saved_episodes')
main()