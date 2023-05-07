import spotipy
import pandas as pd
from google.cloud import bigquery
import os
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s :: %(levelname)s :: %(message)s', filename='logs/current_user_saved_shows.log')

logging.info('The job of getting the saved shows started.')

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

def get_saved_shows(offset = 0):
    saved_shows = sp.current_user_saved_shows(limit=50, offset=offset)

    show_id = []
    show_name = []
    show_description = []
    explicit = []
    show_url = []
    is_externally_hosted = []
    language = []
    type = []
    show_publisher = []
    shwo_total_episodes = []
    show_media_type = []
    added_at= []

    for show in saved_shows["items"]:
        show_id.append(show["show"]["id"])
        show_name.append(show["show"]["name"])
        show_description.append(show["show"]["description"])
        explicit.append(show["show"]["explicit"])
        show_url.append(show["show"]["external_urls"]["spotify"])
        is_externally_hosted.append(show["show"]["is_externally_hosted"])
        language.append(show["show"]["languages"][0])
        type.append(show["show"]["type"])
        show_publisher.append(show["show"]["publisher"])
        shwo_total_episodes.append(show["show"]["total_episodes"])
        show_media_type.append(show["show"]["media_type"])
        added_at.append(show["added_at"])

    shows_dict = {
        "show_id" : show_id,
        "show_name": show_name,
        "show_description" : show_description,
        "explicit" : explicit,
        "show_url" : show_url,
        "is_externally_hosted" : is_externally_hosted,
        "language" : language,
        "type" : type,
        "show_publisher" : show_publisher,
        "shwo_total_episodes" : shwo_total_episodes,
        "show_media_type" : show_media_type,
        "aded_at" : added_at
    }
    return pd.DataFrame(shows_dict, columns = list(shows_dict.keys()))

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
        saved_shows = get_saved_shows()    
        logging.info("Data successfully extracted from API, proceeding to validation stage")
    except:
        logging.exception("Something went wrong while extracting data from API")
        raise

#  TODO -> Update it to request the api until you get all the data using the loop

    if len(saved_shows.index) == 50:
        logging.info("The number of saved show is greater than 50. The offset increased.")
        try:
            saved_shows_second_offset = get_saved_shows(offset=51)
            full = pd.concat([saved_shows, saved_shows_second_offset])
            logging.info("All data successfully extracted from API, proceeding to validation stage")
        except:
            logging.exception("Something went wrong while extracting data from API")
            raise
        
        if check_if_valid_data(full,'show_id'):
            logging.info("Data valid for shows table, proceed to Load stage")            
            load2bq(full,'saved_shows')
        
main()