from etl.s3_upload import get_s3_client, create_bucket_if_not_exists, upload_to_s3, wait_for_s3_file
from utils.constants import AWS_DATA_BUCKET_NAME, AWS_CONN_ID, AWS_REGION
def upload_data_s3_pipeline():
    file_path="/home/ubuntu/airflow/extracted_data_storage_folder/open_weather_data.csv"
    s3_folder_name="weather_raw_data"
    s3=get_s3_client(AWS_CONN_ID)
    create_bucket_if_not_exists(s3, AWS_DATA_BUCKET_NAME, AWS_REGION)
    upload_to_s3(s3, file_path, AWS_DATA_BUCKET_NAME,s3_folder_name,file_path.split('/')[-1])
    wait_for_s3_file(s3,AWS_DATA_BUCKET_NAME,s3_folder_name,file_path.split('/')[-1])

 