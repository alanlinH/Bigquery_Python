
from google.cloud import bigquery as bq
import pandas as pd
import os

# Specify environments
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'key.json'

# 連接Bigquery服務
client = bq.Client()


def query_stackoverflow():    
    # 自訂查詢條件
    query_job = client.query(
        """
        SELECT
          CONCAT(
            'https://stackoverflow.com/questions/',
            CAST(id as STRING)) as url,
          view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE tags like '%google-bigquery%'
        ORDER BY view_count DESC
        LIMIT 10"""
    )

    results = query_job.result()  # Waits for job to complete.

    # 查詢結果
    for row in results:
        print("{} : {} views".format(row.url, row.view_count))


def buildup_table():

    # 設定dataset名稱:
    dataset_id = f"{client.project}.NEW_DATA_SET"
    # 設定table名稱:
    table_id = f"{dataset_id}.TEST_TABLE"
    # 自訂schema
    tschema = [
    bq.SchemaField("full_name", "STRING", mode="NULLABLE"),
    bq.SchemaField("age", "INTEGER", mode="NULLABLE"),
    ]
    # 連接 dataset
    dataset = bq.Dataset(dataset_id)
    dataset.location = "asia-east1"
    # 創建 dataset
    dataset = client.create_dataset(dataset)
    # 連接 table
    table = bq.Table(table_id, schema=tschema)
    # 創建 table
    table = client.create_table(table)  # API request
    
    print(f"Created dataset {client.project}.{dataset.dataset_id}")
    print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")


def upoload_df():
    
    dataset_id = "NEW_DATA_SET"
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table("TEST_TABLE")
    df = pd.DataFrame({u'full_name':['A','B','C','D'],
                       u'age':[200, 100, 30, 4]})
    job = client.load_table_from_dataframe(df, table_ref, location="asia-east1")
    job.result()  # Waits for table load to complete.
    assert job.state == "DONE"
    print('upload DataFrame done!')


if __name__ == "__main__":
    query_stackoverflow()
    buildup_table()
    upoload_df()
