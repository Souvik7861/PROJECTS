from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago


def kelvin_to_Celcius(temp_in_kelvin):
    temp_in_Celcius = (temp_in_kelvin - 273.15)
    return round(temp_in_Celcius, 3)

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="group_a.tsk_extract_pune_weather_data")
    
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_Celcius = kelvin_to_Celcius(data["main"]["temp"])
    feels_like_Celcius= kelvin_to_Celcius(data["main"]["feels_like"])
    min_temp_Celcius = kelvin_to_Celcius(data["main"]["temp_min"])
    max_temp_Celcius = kelvin_to_Celcius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"city": city,
                        "description": weather_description,
                        "temperature_Celcius": temp_Celcius,
                        "feels_like_Celcius": feels_like_Celcius,
                        "minimun_temp_Celcius":min_temp_Celcius,
                        "maximum_temp_Celcius": max_temp_Celcius,
                        "pressure": pressure,
                        "humidity": humidity,
                        "wind_speed": wind_speed,
                        "time_of_record": time_of_record,
                        "sunrise_local_time)":sunrise_time,
                        "sunset_local_time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    
    df_data.to_csv("current_weather_data.csv", index=False, header=False)

def load_weather():
    hook = PostgresHook(postgres_conn_id= 'postgres_conn')
    hook.copy_expert(
        sql= "COPY weather_data FROM stdin WITH DELIMITER as ','",
        filename='current_weather_data.csv'
    )

def save_joined_data_s3(task_instance):
    data = task_instance.xcom_pull(task_ids="task_join_data")
    df = pd.DataFrame(data, columns = ['city', 'description', 'temperature_Celcius', 'feels_like_Celcius', 'minimun_temp_Celcius', 'maximum_temp_Celcius', 'pressure','humidity', 'wind_speed', 'time_of_record', 'sunrise_local_time', 'sunset_local_time', 'state', 'Population', 'land_Area_sq_km'])
    # df.to_csv("joined_weather_data.csv", index=False)
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'joined_weather_data_' + dt_string
    df.to_csv(f"s3://souvik-practice-s3-bucket/{dt_string}.csv", index=False)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':  days_ago(0),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


with DAG('weather_ETL',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        start_pipeline = DummyOperator(
            task_id = 'tsk_start_pipeline'
        )

        join_data = PostgresOperator(
                task_id='task_join_data',
                postgres_conn_id = "postgres_conn",
                sql= '''SELECT 
                    w.city,                    
                    description,
                    temperature_Celcius,
                    feels_like_Celcius,
                    minimun_temp_Celcius,
                    maximum_temp_Celcius,
                    pressure,
                    humidity,
                    wind_speed,
                    time_of_record,
                    sunrise_local_time,
                    sunset_local_time,
                    state,
                    Population, 
                    land_Area_sq_km                    
                    FROM weather_data w
                    INNER JOIN city_look_up c
                        ON w.city = c.city                                      
                ;
                '''
            )

        load_joined_data = PythonOperator(
            task_id= 'task_load_joined_data',
            python_callable=save_joined_data_s3
            )

        end_pipeline = DummyOperator(
                task_id = 'task_end_pipeline'
        )

        with TaskGroup(group_id = 'group_a', tooltip= "Extract_from_S3_and_weatherapi") as group_A:
            create_table_1 = PostgresOperator(
                task_id='tsk_create_table_1',
                postgres_conn_id = "postgres_conn",
                sql= '''  
                    CREATE TABLE IF NOT EXISTS city_look_up (
                    city TEXT NOT NULL,
                    state TEXT NOT NULL,
                    Population numeric NOT NULL,
                    land_Area_sq_km numeric NOT NULL                    
                );
                '''
            )

            truncate_table = PostgresOperator(
                task_id='tsk_truncate_table',
                postgres_conn_id = "postgres_conn",
                sql= ''' TRUNCATE TABLE city_look_up;
                    '''
            )

            uploadS3_to_postgres  = PostgresOperator(
                task_id = "tsk_uploadS3_to_postgres",
                postgres_conn_id = "postgres_conn",
                sql = "SELECT aws_s3.table_import_from_s3('city_look_up', '', '(format csv, HEADER true)', 'souvik-practice-s3-bucket', 'city_info.csv', 'ap-south-1');"
            )

            create_table_2 = PostgresOperator(
                task_id='tsk_create_table_2',
                postgres_conn_id = "postgres_conn",
                sql= ''' 
                    CREATE TABLE IF NOT EXISTS weather_data (
                    city TEXT,
                    description TEXT,
                    temperature_Celcius NUMERIC,
                    feels_like_Celcius NUMERIC,
                    minimun_temp_Celcius NUMERIC,
                    maximum_temp_Celcius NUMERIC,
                    pressure NUMERIC,
                    humidity NUMERIC,
                    wind_speed NUMERIC,
                    time_of_record TIMESTAMP,
                    sunrise_local_time TIMESTAMP,
                    sunset_local_time TIMESTAMP                    
                );
                '''
            )

            is_pune_weather_api_ready = HttpSensor(
                task_id ='tsk_is_pune_weather_api_ready',
                http_conn_id='weathermap_api',
                endpoint='/data/2.5/weather?q=pune&APPID=658e07e7db04258e3cf61c41a0d2aa46'
            )

            extract_pune_weather_data = SimpleHttpOperator(
                task_id = 'tsk_extract_pune_weather_data',
                http_conn_id = 'weathermap_api',
                endpoint='/data/2.5/weather?q=pune&APPID=658e07e7db04258e3cf61c41a0d2aa46',
                method = 'GET',
                response_filter= lambda r: json.loads(r.text),
                log_response=True
            )

            transform_load_pune_weather_data = PythonOperator(
                task_id= 'transform_load_pune_weather_data',
                python_callable=transform_load_data
            )

            load_weather_data = PythonOperator(
            task_id= 'tsk_load_weather_data',
            python_callable=load_weather
            )



            create_table_1 >> truncate_table >> uploadS3_to_postgres
            create_table_2 >> is_pune_weather_api_ready >> extract_pune_weather_data >> transform_load_pune_weather_data >> load_weather_data
        start_pipeline >> group_A >> join_data >> load_joined_data >> end_pipeline