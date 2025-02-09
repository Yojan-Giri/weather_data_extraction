
# OpenWeather Data Extraction with Airflow

This project automates the extraction of current weather data for various cities using the OpenWeather API. The data is fetched, processed, saved into a file, and then uploaded to an Amazon S3 bucket, all orchestrated through Apache Airflow.

## Project Overview

The goal of this project is to pull real-time weather data from OpenWeather for different cities, save it to a file, and upload it to an S3 bucket for future analysis or monitoring.

### Steps Involved:

1. **Data Extraction**:
   - The weather data for a given city is fetched from the OpenWeather API, which includes temperature, wind speed, humidity, pressure, and other weather-related details.
   
2. **Data Transformation**:
   - The extracted data is parsed and organized into a structured dictionary, ensuring relevant weather information like temperature, humidity, visibility, and wind details are captured.
   
3. **Save to File**:
   - The structured data is saved into a file format such as JSON or CSV, making it easier to handle and analyze.
   
4. **Upload to S3**:
   - The resulting file is then uploaded to an Amazon S3 bucket for storage and future retrieval.
   
5. **Automation with Airflow**:
   - Airflow is used to schedule and automate the entire process, ensuring data extraction and file uploads happen regularly without manual intervention.

## Features:
- Extracts weather data for multiple cities.
- Supports real-time weather conditions, including temperature, humidity, wind speed, visibility, and more.
- Stores data in a file format that is compatible with further analysis.
- Automates the entire workflow using Apache Airflow.
- Saves processed files to Amazon S3 for easy access and long-term storage.

## Requirements:
- Python 3.x
- Apache Airflow
- OpenWeather API key
- AWS credentials for S3 access
- Spark



## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/Yojan-Giri/taxi_data_automation_airflow.git
cd taxi_data_automation_airflow
```
### 2. Run the Setup Script

```bash
bash setup.sh
```
### 3. Start Apache Airflow Services

```bash
airflow standalone
```
This command automatically initializes Airflow, starts the webserver, scheduler, and creates an admin user.

### 4. Access Airflow Web UI
Open your browser and go to:

```bash
http://localhost:8080
```

### 5. Default Login Credentials

After running airflow standalone, Airflow will generate a default admin user. You’ll see the login credentials in the terminal.

### 6. Running the DAGs

1. Place your DAG Python files inside the `airflow/dags` directory.

2. Restart the scheduler to detect new DAGs:

   ```bash
   airflow standalone
   ```
3. Open the Airflow UI and enable the DAGs.

### 7. Set Up AWS Connection in Airflow  

1. Open [Airflow Web UI](http://localhost:8080)  
2. Go to **Admin** > **Connections** > **+ Add Connection**  
3. Set:  
   - **Connection Id**: `aws_default`  
   - **Connection Type**: `Amazon Web Services`  
   - **Login**: *AWS Access Key ID*  
   - **Password**: *AWS Secret Access Key*  
   - **Extra**:  
     ```json
     { "region_name": "eu-north-1" }
     ```
   *(Replace `"eu-north-1"` with your AWS region)*  
4. Click **Save**  


### 8.  Troubleshooting

If Airflow doesn't start, check the logs:

```bash
airflow standalone --debug
```
