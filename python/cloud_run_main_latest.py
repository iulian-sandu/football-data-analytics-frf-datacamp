import base64
import functions_framework # type: ignore
import json
import datetime
from google.cloud import storage
from google.cloud import bigquery
import random

def data_transformation():
    """
    Transforms data in BigQuery by calculating average statistics for each team.
    The results are stored in a new table.
    """
    sql = """
        SELECT 
            t.name as team_name,
            AVG(t.statistics.wins) AS avg_wins,
            AVG(t.statistics.draws) AS avg_draws,
            AVG(t.statistics.losses) AS avg_losses
        FROM 
            `spatial-tempo-425409-i2.main_dataset.auto_upload_table` as t
        GROUP BY
            t.name;
    """
    client = bigquery.Client()
    table_ref = f"spatial-tempo-425409-i2.main_dataset.auto_filtered_latest"

    job_config = bigquery.QueryJobConfig(
        destination=table_ref,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    query_job = client.query(sql, job_config=job_config)

    return query_job.result()

def upload_bigquery(filename):
    """
    Uploads new data to a BigQuery table from a file in Google Cloud Storage.
    The file is expected to be in newline-delimited JSON format.
    The table schema is automatically detected.
    The data is appended to the existing table.
    """
    client = bigquery.Client()
    table_id = "spatial-tempo-425409-i2.main_dataset.auto_upload_table"

    job_config = bigquery.LoadJobConfig(
        autodetect=True, 
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, 
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    uri = f"gs://frf-datacamp/auto-scraped-files/{filename}"
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    return

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a file with dummy data to the specified Google Cloud Storage bucket.
    The file is named with a unique identifier based on the current timestamp.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    
    generation_match_precondition = 0

    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    """
    Main function triggered from a message on a Cloud Pub/Sub topic.
    The function decodes the message, checks if it indicates a job start
    and then processes the data accordingly.
    """
    pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
    
    random_month = random.randint(1, 12)
    random_wins = random.randint(1, 20)
    random_draws = random.randint(1, 5)
    random_losses = random.randint(2, 6)

    dummy_data = {
        "id": 1,
        "name": "Dinamo",
        "league": "Liga 1",
        "month": random_month,
        "statistics": {
            "matches_played": 30,
            "wins": random_wins,
            "draws": random_draws,
            "losses": random_losses,
            "goals_for": 60,
            "goals_against": 30
        }
    }

    unique_id = f"dinamo_statistics_2023_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

    if pubsub_message == "job_started":
        try:
            with open(f'{unique_id}_processed.jsonl', 'w') as f:
                f.write(json.dumps(dummy_data) + '\n')

            upload_blob('frf-datacamp', f'{unique_id}_processed.jsonl', f'auto-scraped-files/{unique_id}_processed.jsonl')
            print('Statistics for Dinamo uploaded successfully.')

            upload_bigquery(f'{unique_id}_processed.jsonl')
            print('Data uploaded to BigQuery successfully.')

            data_transformation()
            print('Data transformation completed successfully.')
        except Exception as e:
            print(f"An unexpected error occurred: {e}")  
    else:
        print("Invalid pub/sub body.")
