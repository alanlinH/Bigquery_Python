# Bigquery_Python

> GCP上操作 (第一次設定 : 服務帳戶設定與金鑰產生):  
* 在 Google Cloud 控制台的項目選擇器頁面上，選擇或創建一個 Google Cloud 項目。  
* 啟用 BigQuery API  
* 創建服務帳戶  
* 創建服務帳戶密鑰  

> 本地端操作 :  
* 在虛擬環境中安裝:  
  * pip install --upgrade google-cloud-bigquery  
  * pip install pandas
* bq_connect.py
  * query_stackoverflow( ) : Query 方式  
  * buildup_table( ) : 建立dataset與table  
  * upoload_df( ) : 上傳自己的DataFrame
