from google.cloud import bigquery
import pandas as pd
import os

def load2bq(data: pd.DataFrame, table_id: str, load_type: str = "WRITE_TRUNCATE"):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""

    project_id = ""
    dataset_id = ""
    table_id = table_id

    client = bigquery.Client(project=project_id)
    dataset = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=load_type,
    )

    try:
        client.load_table_from_dataframe(data, table, job_config=job_config)
        print(f"Loaded {len(data)} rows to {table}")
    except:
        print("Something went wrong while loading data to BigQuery")
        raise