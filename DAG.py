from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import json

def kelvin_to_celsius(kelvin_temp):
    return kelvin_temp - 273.15

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")

    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_celsius = kelvin_to_celsius(data['main']["temp"])
    feels_like_celsius = kelvin_to_celsius(data["main"]["feels_like"])
    min_temp_celsius = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_celsius = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (C)": temp_celsius,
        "Feels Like (C)": feels_like_celsius,
        "Minimum Temp (C)": min_temp_celsius,
        "Maximum Temp (C)": max_temp_celsius,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time
    }

    return transformed_data

def save_to_json(task_instance, json_data):
    json_file_path = '/home/ubuntu/airflow/dags/current_weather_data_mumbai.json'
    try:
        with open(json_file_path, 'w') as f:
            json.dump(json_data, f, default=str)
        print("JSON file saved successfully.")
    except Exception as e:
        print(f"Error saving JSON file: {e}")
    return json_file_path


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 17),
    'email': ['architsood99@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG("weather_dag", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    weather_api_ready = HttpSensor(
        task_id="weather_api_ready",
        http_conn_id="weather_api",
        endpoint="/data/2.5/weather?lat=19.0760&lon=72.8777&appid=46c43dca514d1dfa379d9ee32f4baead",
        timeout=20,
        mode="poke"
    )

    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weather_api',
        endpoint="/data/2.5/weather?lat=19.0760&lon=72.8777&appid=46c43dca514d1dfa379d9ee32f4baead",
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data,
        provide_context=True
    )

    save_to_json_task = PythonOperator(
        task_id='save_to_json',
        python_callable=save_to_json,
        op_args=['{{ ti.xcom_pull(task_ids="transform_load_weather_data") }}'],
        provide_context=True
    )

    weather_api_ready >> extract_weather_data >> transform_load_weather_data >> save_to_json_task
