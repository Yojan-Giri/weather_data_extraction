import configparser
import os

# Load configuration
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'config.conf'))




# AWS Configuration
AWS_CONN_ID = config.get("aws", "aws_conn_id")
AWS_DATA_BUCKET_NAME = config.get("aws", "aws_data_bucket_name")
AWS_REGION=config.get("aws", "aws_region")

# Glue Configuration
IAM_ROLE = config.get("glue", "iam_role")
S3_INPUT_TARGET_PATH = config.get("glue", "s3_input_target_path")


#OpenWeatherConfiguration
API_KEY=config.get("openweather", "api_key")
BASE_URL=config.get("openweather", "base_url")