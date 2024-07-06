import os

from google.cloud import bigquery
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'project-solar-power-22535f139771.json'

client = bigquery.Client()

project_id = 'project-solar-power'
dataset_id = 'project-solar-power.plant_1_generation_data.plant 1'
table_id = "project-solar-power.plant_1_generation_data.plant_1_data"

csv_files = [f for f in os.listdir('final') if f.endswith('.csv')]
if len(csv_files) != 1:
    raise ValueError('should be only one csv file in the current directory')

filename = csv_files[0]

df = pd.read_csv('final/' + filename)
df['timestamp-from'] = pd.to_datetime(df['timestamp-from'])
df['timestamp-to'] = pd.to_datetime(df['timestamp-to'])
df['plant_id'] = df['plant_id'].astype(str)

job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
client.load_table_from_dataframe(df, table_id, job_config=job_config)
print(f"Data loaded into {project_id}.{dataset_id}.{table_id}")
