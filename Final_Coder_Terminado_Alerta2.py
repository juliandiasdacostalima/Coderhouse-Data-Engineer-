
import requests
import psycopg2
import json
from datetime import datetime, date, timedelta
import airflow.utils.dates
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
import smtplib
import configparser
import os

def cargar_configuracion():
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def alerta_tabla(**kwargs):
    conn = BaseHook.get_connection('redshift_coder')
    conn_str = f"host={conn.host} port={conn.port} dbname={conn.schema} user={conn.login} password={conn.password}"
    current_date = date.today()
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MAX(time) FROM Fact_Historial_Clima")
            last_date = cursor.fetchone()[0]
            last_date_str = str(last_date.strftime("%Y-%m-%d"))  # Convertir a cadena
            kwargs['ti'].xcom_push(key='last_date', value=last_date_str)   # Empujar cadena en lugar de objeto date
    if last_date <= current_date + timedelta(days=4):
        enviar_correo_base_desactualizada()
        print('La base se encuentra desactualizada')
    else:
        print('La base se encuentra actualizada')

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

def enviar_correo_base_desactualizada(context=None):
    subject = 'Base desactualizada'
    body_text = 'La tabla Fact_Historial_Clima se encuentra desactualizada'
    enviar_email(subject, body_text)

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id="Alerta",
    default_args=args,
    schedule_interval='0 0 * * *',  # Ejecutar todos los días a la medianoche
)


alerta_tabla = PythonOperator(
    task_id='Alerta',
    python_callable=alerta_tabla,
    provide_context=True,
    dag=dag
)

alerta_tabla