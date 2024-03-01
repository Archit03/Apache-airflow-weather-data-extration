# Weather Data Processing DAG

## Overview
This Airflow DAG fetches weather data from an external API, transforms it into a structured format, and saves it to a JSON file. It operates on a daily schedule to ensure the weather data is regularly updated and available for analysis or consumption by downstream systems.

## Components

### 1. Default Arguments
- **Owner**: Specifies the owner of the DAG.
- **Depends on Past**: Determines whether the DAG depends on past runs.
- **Start Date**: Defines the start date for DAG execution.
- **Email**: Specifies the email address to send notifications to.
- **Email on Failure**: Indicates whether to send an email notification on task failure.
- **Email on Retry**: Specifies whether to send an email notification on task retries.
- **Retries**: Number of retries for failed tasks.
- **Retry Delay**: Time interval between retries.

### 2. DAG Configuration
- **DAG ID**: "weather_dag"
- **Schedule Interval**: "@daily" (Runs once a day)
- **Catchup**: Disabled (Does not run missed DAG runs)

### 3. Tasks

#### a. Weather API Ready (HttpSensor)
- **Task ID**: "weather_api_ready"
- **Description**: Checks if the weather API is ready and accessible.
- **HTTP Connection ID**: "weather_api"
- **Endpoint**: Endpoint to check API readiness.
- **Timeout**: Maximum time to wait for API response.
- **Mode**: "poke" (Polls the API until it's ready)

#### b. Extract Weather Data (SimpleHttpOperator)
- **Task ID**: "extract_weather_data"
- **Description**: Fetches weather data from the external API.
- **HTTP Connection ID**: "weather_api"
- **Endpoint**: API endpoint to fetch weather data.
- **Method**: "GET"
- **Response Filter**: Lambda function to filter and parse API response.
- **Log Response**: Whether to log the API response.

#### c. Transform & Load Weather Data (PythonOperator)
- **Task ID**: "transform_load_weather_data"
- **Description**: Transforms the fetched weather data into a structured format.
- **Python Callable**: transform_load_data function
- **Provide Context**: True (Provides context to the Python function)

#### d. Save to JSON (PythonOperator)
- **Task ID**: "save_to_json"
- **Description**: Saves the transformed weather data to a JSON file.
- **Python Callable**: save_to_json function
- **Provide Context**: True (Provides context to the Python function)

### 4. Task Dependencies
- **weather_api_ready** >> **extract_weather_data**: Ensure weather API is ready before extracting data.
- **extract_weather_data** >> **transform_load_weather_data**: Transform the extracted data after fetching.
- **transform_load_weather_data** >> **save_to_json**: Save the transformed data to JSON after transformation.

## Execution
- The DAG runs daily, triggered by the specified schedule interval.
- Each task executes sequentially, with dependencies ensuring proper execution flow.
- Email notifications are sent on task failure, as per the configured settings.
