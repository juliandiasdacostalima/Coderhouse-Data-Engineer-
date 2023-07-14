import requests
import psycopg2
import json
from datetime import datetime, date,timedelta
import airflow.utils.dates
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
import smtplib
import configparser
import os


""" Establecí un archivo config.ini, en este caso puse que el archivo debe encontrarse en el mismo lugar del DAG pero entiendo que esto es mejor separarlo y ponerlo en una carpeta config 
, este archivo contiene las credenciales de la db y de mi mail de google"""

def cargar_configuracion():
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

""" Generé una función max_date la cual consulta la máxima fecha cargada en mi tabla, esta fecha es importante ya que cargaré nuevos registros en base a esta fecha """

def max_date(**kwargs):
    config = cargar_configuracion()
    conn_str = f"host={config.get('redshift_coder', 'host')} port={config.get('redshift_coder', 'port')} dbname={config.get('redshift_coder', 'dbname')} user={config.get('redshift_coder', 'user')} password={config.get('redshift_coder', 'password')}"
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(time) FROM Fact_Historial_Clima where time <= getdate()")
            last_date = cursor.fetchone()[0]
            last_date_delta = last_date - timedelta(days = 7)
            last_date_str = str(last_date_delta.strftime("%Y-%m-%d"))  # Convertir a cadena
            kwargs['ti'].xcom_push(key='last_date', value=last_date_str)  # Empujar cadena en lugar de objeto date

""" Para realizar la extracción de datos de la API, se utilizará como parámetro la fecha obtenida en la función max_date y la fecha de hoy más x días, en mi caso 7"""

def make_api_request(**kwargs):    
    url = "https://api.open-meteo.com/v1/forecast"
    current_date = datetime.now().date() + timedelta(days=7)
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
    return response

"""Para hacer la inserción, se deben tener en cuenta algunas cosas:si el registro ya existe se actualiza, si no existe entonces se inserta, esto se debe a 2 razones:
 en primer lugar muchas fechas son futuras, a medida que pasan los días, las fechas se convierten en eventos anteriores, por ende los datos deben ser ajustados a la realidad,
por otro lado, no es lo mismo predecir un clima con 7 días de anticipación que con 1 día de anticipación, por ende se ajustan los valores futuros """

def insert_into_db(**kwargs):
    config = cargar_configuracion()
    conn_str = f"host={config.get('redshift_coder', 'host')} port={config.get('redshift_coder', 'port')} dbname={config.get('redshift_coder', 'dbname')} user={config.get('redshift_coder', 'user')} password={config.get('redshift_coder', 'password')}"
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            response = make_api_request(**kwargs)
            data_json = response.json()
            current_timestamp = datetime.now()
            query = "INSERT INTO Fact_Historial_Clima (time, apparent_temperature_min, apparent_temperature_max, temperature_2m_max, temperature_2m_min, precipitation_probability_max, precipitation_hours, precipitation_sum, showers_sum, rain_sum, sunrise, sunset) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            query_update = "UPDATE Fact_Historial_Clima SET apparent_temperature_min = %s, apparent_temperature_max = %s, temperature_2m_max = %s, temperature_2m_min = %s, precipitation_probability_max = %s, precipitation_hours = %s, precipitation_sum = %s, showers_sum = %s, rain_sum = %s, sunrise = %s, sunset = %s WHERE time = %s"
            
            print('va esto')
            print((data_json["daily"]["time"]))
            for i in range(len(data_json["daily"]["time"])):
                time = data_json["daily"]["time"][i]
                print(time)
                apparent_temperature_min = data_json["daily"]["apparent_temperature_min"][i]
                apparent_temperature_max = data_json["daily"]["apparent_temperature_max"][i]
                temperature_2m_max = data_json["daily"]["temperature_2m_max"][i]
                temperature_2m_min = data_json["daily"]["temperature_2m_min"][i]
                precipitation_probability_max = data_json["daily"]["precipitation_probability_max"][i]
                precipitation_hours = data_json["daily"]["precipitation_hours"][i]
                precipitation_sum = data_json["daily"]["precipitation_sum"][i]
                showers_sum = data_json["daily"]["showers_sum"][i]
                rain_sum = data_json["daily"]["rain_sum"][i]
                sunrise = data_json["daily"]["sunrise"][i]
                sunset = data_json["daily"]["sunset"][i]

                # Verificar si el registro ya existe para la fecha actual
                cursor.execute("SELECT time FROM Fact_Historial_Clima WHERE time = %s", (time,))
                existing_record = cursor.fetchone()
                print(existing_record)
                
                if existing_record:
                    print('Se actualiza')
                    # Actualizar el registro existente
                    cursor.execute(
                        query_update,
                        (
                            apparent_temperature_min,
                            apparent_temperature_max,
                            temperature_2m_max,
                            temperature_2m_min,
                            precipitation_probability_max,
                            precipitation_hours,
                            precipitation_sum,
                            showers_sum,
                            rain_sum,
                            sunrise,
                            sunset,
                            time,
                        ),
                    )
                else:
                    print('No se actualiza nada')
                    # Insertar un nuevo registro
                    cursor.execute(
                        query,
                        (
                            time,
                            apparent_temperature_min,
                            apparent_temperature_max,
                            temperature_2m_max,
                            temperature_2m_min,
                            precipitation_probability_max,
                            precipitation_hours,
                            precipitation_sum,
                            showers_sum,
                            rain_sum,
                            sunrise,
                            sunset
                        ),
                    )
                
"""Generé una función enviar_mail, la cual es genérica y el asunto y el mensaje contenido serán parámetros que se deben envíar, esto lo hice debido a que solo llegarán mensajes de error,
pero de ser necesario, también se pueden utilizar mensajes de éxito, por ende este enviar_mail se puede usar para cualquier tipo de mail, solo es necesario pasarle parámetros"""

def enviar_email(subject, body):
    x = smtplib.SMTP('smtp.gmail.com', 587)
    x.starttls()
    config = cargar_configuracion()
    username = config.get('Email', 'username')
    password = config.get('Email', 'password')
    x.login(username, password)
    message = f"Subject: {subject}\n\n{body}"
    x.sendmail(username, username, message)
    print('Exito')

def enviar_correo_fallo(context=None):
    subject = 'Error'
    body_text = 'Se generó un error al intentar realizar '
    enviar_email(subject, body_text)

def enviar_correo_exito():
    subject = 'DAG finaliza correctamente'
    body_text = 'El DAG ha finalizado correctamente'
    enviar_email(subject, body_text)

"""Le agregué el parámetro enviar_correo_fallo en caso de que el DAG falle por cualquier motivo"""
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'on_failure_callback': enviar_correo_fallo
}

dag = DAG(
    dag_id="ETL_API",
    default_args=args,
    schedule_interval='0 0 * * *',  # Ejecutar todos los días a la medianoche
)

"""Esta tarea obtiene la fecha máxima y la carga en un XCOM"""
tarea_obtener_fecha = PythonOperator(
    task_id='obtener_fecha',
    python_callable=max_date,
    provide_context=True,
    dag=dag
)
"""Esta tarea inserta los registros en la base de datos"""

tarea_insertar = PythonOperator(
    task_id='insertar_db',
    python_callable=insert_into_db,
    provide_context=True,
    dag=dag
)

"""La siguiente tarea está comentada, es el caso en que el DAG se corrió automáticamente, en mi caso solo necesito que me envíe mensajes de error, pero si se descomenta la línea
se enviaran mensajes en caso de que el DAG se ejecute correctamente"""
""" tarea_enviar_correo = PythonOperator(
    task_id='dag_envio',
    python_callable=enviar_correo_exito,
    dag=dag
) """

tarea_obtener_fecha >> tarea_insertar