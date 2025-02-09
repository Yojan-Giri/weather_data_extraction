import requests
import json
from datetime import datetime

def extract_open_weather_data(city, API_KEY, BASE_URL):
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"  # Get temperature in Celsius
    }

    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        main = data["main"]
        wind = data["wind"]
        sys = data["sys"]
        weather = data["weather"][0]  # Access first item in the weather list

        return {
            "city": city,
            "weather": weather["main"],
            "weather_description": weather["description"],
            "temperature": float(main["temp"]),
            "feels_like": float(main["feels_like"]),
            "temp_min": float(main["temp_min"]),
            "temp_max": float(main["temp_max"]),
            "pressure": int(main["pressure"]),
            "humidity": int(main["humidity"]),
            "sea_level": int(main.get("sea_level")) if main.get("sea_level") is not None else None,
            "grnd_level": int(main.get("grnd_level")) if main.get("grnd_level") is not None else None,
            "visibility": int(data.get("visibility")) if data.get("visibility") is not None else None,
            "wind_speed": float(wind["speed"]),
            "wind_deg": int(wind["deg"]),
            "country": sys["country"],
            "time_of_record": datetime.utcfromtimestamp(data["dt"] + data["timezone"]),
            "sunrise": datetime.utcfromtimestamp(sys["sunrise"] + data["timezone"]),
            "sunset": datetime.utcfromtimestamp(sys["sunset"] + data["timezone"]),
        }
    else:
        print(f"Failed to fetch data for {city}: {response.text}")
        return None
