import spotipy
import pandas as pd
from google.cloud import bigquery
import os

sp = spotipy.Spotify(
    auth_manager=spotipy.oauth2.SpotifyOAuth(client_id = "", 
    client_secret = "", 
    redirect_uri = "http://localhost:7777/callback", 
    scope="user-library-read")
)

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")
    return True

def get_genres():
    genres = sp.recommendation_genre_seeds()
    return pd.DataFrame(genres, columns = list(genres.keys()))

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

    job = client.load_table_from_dataframe(data, table, job_config=job_config)
    print(f"Loaded {len(data.index)} rows to {table}")
    
def main():
    genres = get_genres()    

    if check_if_valid_data(genres):
        print("Data valid for genres table, proceed to Load stage")  
        load2bq(genres,'genres')

main()