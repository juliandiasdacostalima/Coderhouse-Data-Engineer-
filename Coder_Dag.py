# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 17:46:39 2023

@author: julia
"""

import requests
import psycopg2
import json
import configparser
from datetime import datetime
import airflow.utils.dates
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

config = configparser.ConfigParser()
config.read("config.ini")
conn = BaseHook.get_connection('redshift_coder')



def max_date(conn=conn, **kwargs):
    conn = BaseHook.get_connection('redshift_coder')
    conn_str = f"host={conn.host} port={conn.port} dbname={conn.schema} user={conn.login} password={conn.password}"
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(time) FROM Fact_Historial_Clima")
            last_date = cursor.fetchone()[0]
            last_date_str = str(last_date.strftime("%Y-%m-%d"))  # Convertir a cadena
            kwargs['ti'].xcom_push(key='last_date', value=last_date_str)  # Empujar cadena en lugar de objeto date
    #return last_date

def make_api_request(**kwargs):
    url = "https://api.open-meteo.com/v1/forecast"
    current_date = datetime.now().date()
    
    #BaseHook.get_connection(conn_id)
    #last_date = max_date()
    last_date = kwargs['ti'].xcom_pull(key='last_date', task_ids='obtener_fecha')
    last_date = datetime.strptime(last_date, "%Y-%m-%d").date()
    params = {
        "latitude": "34.500",
        "longitude": "-58.56",
        "timezone": "America/Argentina/Buenos_Aires",
        "daily": [
            "apparent_temperature_min",
            "apparent_temperature_max",
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_probability_max",
            "precipitation_hours",
            "precipitation_sum",
            "showers_sum",
            "rain_sum",
            "sunrise",
            "sunset",
        ],
        "start_date": last_date.strftime("%Y-%m-%d") if last_date else "2022-12-01",
        "end_date": current_date,
    }
    
    response = requests.get(url, params=params)
    # if response.status_code == 200:  # Verificar si la solicitud fue exitosa
    #     response_text = response.text
    #     kwargs['ti'].xcom_push(key='api_response', value=response_text)
    # else:
    #     print("API request failed with status code:", response.status_code)
    return response
    
def insert_into_db(conn=None, **kwargs):
    conn = BaseHook.get_connection('redshift_coder')
    conn_str = f"host={conn.host} port={conn.port} dbname={conn.schema} user={conn.login} password={conn.password}"
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            response = make_api_request(**kwargs)
            data_json = response.json()
            
            query = "INSERT INTO Fact_Historial_Clima (time, apparent_temperature_min, apparent_temperature_max, temperature_2m_max, temperature_2m_min, precipitation_probability_max, precipitation_hours, precipitation_sum, showers_sum, rain_sum, sunrise, sunset) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            
            for i in range(len(data_json["daily"]["time"])):
                cursor.execute(
                    query,
                    (
                        data_json["daily"]["time"][i],
                        data_json["daily"]["apparent_temperature_min"][i],
                        data_json["daily"]["apparent_temperature_max"][i],
                        data_json["daily"]["temperature_2m_max"][i],
                        data_json["daily"]["temperature_2m_min"][i],
                        data_json["daily"]["precipitation_probability_max"][i],
                        data_json["daily"]["precipitation_hours"][i],
                        data_json["daily"]["precipitation_sum"][i],
                        data_json["daily"]["showers_sum"][i],
                        data_json["daily"]["rain_sum"][i],
                        data_json["daily"]["sunrise"][i],
                        data_json["daily"]["sunset"][i],
                    ),
                )
    
    #close_connection(conn)
#%%

dag = DAG(
    dag_id="coder_dag",
    default_args=args,
    schedule_interval='0 0 * * *',  # Ejecutar todos los días a la medianoche
)
    
tarea_obtener_fecha = PythonOperator(
task_id='obtener_fecha',
python_callable=max_date,
provide_context=True,
dag=dag
)
    
# tarea_solicitud_api = PythonOperator(
# task_id='solicitud_api',
# python_callable=make_api_request,
# provide_context=True,
# dag=dag
# )

tarea_insertar = PythonOperator(
task_id='insertar_db',
python_callable=insert_into_db,
provide_context=True,
dag=dag
)

tarea_obtener_fecha  >> tarea_insertar
