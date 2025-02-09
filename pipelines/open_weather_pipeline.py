from etl.open_weather_etl import extract_open_weather_data
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType
import json
from datetime import datetime
import os
import shutil
from utils.constants import BASE_URL, API_KEY




def extract_open_weather_pipeline():
    CITIES = [
        "Kabul", "Yerevan", "Canberra", "Vienna", "Baku", "Manama", "Dhaka", "Brussels", "Belmopan", "Thimphu",
        "La Paz", "Sarajevo", "Brasília", "Sofia", "Ouagadougou", "Phnom Penh", "Yaoundé", "Ottawa", "Praia",
        "Bangui", "Santiago", "Beijing", "Bogotá", "San José", "Zagreb", "Havana", "Nicosia", "Copenhagen", 
        "Quito", "Cairo", "San Salvador", "Malabo", "Asmara", "Tallinn", "Addis Ababa", "Suva", "Helsinki", "Paris",
        "Libreville", "Banjul", "Tbilisi",  "Accra",  "Guatemala City", "Conakry", "Georgetown", "Port au Prince",
        "Tegucigalpa",  "Reykjavik", "New Delhi", "Jakarta", "Tehran", "Baghdad", "Dublin", "Jerusalem", 
        "Tokyo", "Amman", "Astana", "Nairobi", "Pristina", "Kuwait City", "Bishkek", "Vientiane", "Beirut", "Maseru",
        "Monrovia", "Tripoli", "Vilnius", "Luxembourg", "Antananarivo", "Lilongwe", "Kuala Lumpur", "Malé", "Bamako", "Valletta",
        "Nouakchott", "Port Louis", "Mexico City", "Chișinău", "Ulaanbaatar", "Rabat", "Maputo", "Windhoek", "Kathmandu",
        "Wellington", "Managua", "Niamey", "Abuja", "Pyongyang",  "Muscat", "Islamabad", "Panama City", "Asunción",
        "Lima", "Manila", "Warsaw", "Lisbon", "Doha",  "Moscow", "Kigali", "Basseterre", "Castries", "Kingstown", "Apia",
        "San Marino", "São Tomé", "Dakar",  "Victoria", "Freetown", "Singapore", "Bratislava", "Ljubljana", "Mogadishu",
        "Pretoria", "Seoul", "Juba", "Madrid", "Colombo", "Khartoum", "Paramaribo", "Stockholm", "Bern", "Damascus", "Taipei",
        "Dushanbe", "Dodoma", "Bangkok", "Lomé", "Port of Spain", "Tunis", "Ankara", "Ashgabat", "Kampala", "Kyiv", "Abu Dhabi",
        "London",  "Montevideo", "Tashkent", "Caracas", "Hanoi", "Sanaa", "Lusaka", "Harare", "New York",
        "Los Angeles", "Chicago", "Boston", "San Francisco", "Madrid", "Rome", "Berlin",  "Amsterdam",
        "Athens",    "Prague",   "Budapest", 
        "Zagreb", "Bucharest",  "Riga", "Belgrade", "Podgorica",  "Oslo"
    ]

    weather_data = [extract_open_weather_data(city,  API_KEY,BASE_URL) for city in CITIES if extract_open_weather_data(city, API_KEY,BASE_URL)]
    
    spark=SparkSession.builder.appName('weather_data').getOrCreate()

    weather_schema = StructType([
    StructField("city", StringType(), True),
    StructField("weather", StringType(), True),
    StructField("weather_description", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("feels_like", DoubleType(), True),
    StructField("temp_min", DoubleType(), True),
    StructField("temp_max", DoubleType(), True),
    StructField("pressure", IntegerType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("sea_level", IntegerType(), True),
    StructField("grnd_level", IntegerType(), True),
    StructField("visibility", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_deg", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("time_of_record", TimestampType(), True),
    StructField("sunrise", TimestampType(), True),
    StructField("sunset", TimestampType(), True)
    ])

    df_weather=spark.createDataFrame(data=weather_data, schema=weather_schema)

    # Including the local time and location as a field

    df_weather=df_weather.withColumn("Current_local_time",current_timestamp())
    df_weather=df_weather.withColumn("local_location",lit("NL"))
    
    # Write the Dataframe as a csv
    file_path="/home/ubuntu/airflow/extracted_data_storage_folder/"
    df_weather.coalesce(1).write.mode("overwrite").csv(path=file_path, header=True)


    # List all files in the target folder
    part_files = [f for f in os.listdir(file_path) if f.startswith("part-0000") and f.endswith(".csv")]

    # If exactly one part file is found, proceed
    if len(part_files) == 1:
        part_file = os.path.join(file_path, part_files[0])  
        final_file_name = "open_weather_data.csv"
        
        # Rename the part file to the final file name
        shutil.move(part_file, final_file_name)
        
        # Remove all other files in the target folder (except the final renamed file)
        for f in os.listdir(file_path):
            file_path_to_remove = os.path.join(file_path, f)
            if f != part_files[0]:  # Skip the renamed part file
                os.remove(file_path_to_remove)  # Delete the other files
    else:
        print(f"Error: Expected exactly one part file, found {len(part_files)}.")
    







    
