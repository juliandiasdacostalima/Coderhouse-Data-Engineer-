# -*- coding: utf-8 -*-
"""
Created on Mon May 15 21:46:11 2023

@author: julia
"""

import requests
import psycopg2
import json
import configparser


config = configparser.ConfigParser()
config.read('config.ini')

conn = psycopg2.connect(
    database=config.get('database', 'db_name'),
    user=config.get('database', 'db_user'),
    password=config.get('database', 'db_password'),
    host=config.get('database', 'db_host'),
    port=config.getint('database', 'db_port')
)



#%%

# Registro a insertar
url = "https://api.open-meteo.com/v1/forecast"

# Definir los parámetros de la solicitud
params = {
    "latitude": "34.500",
    "longitude": "-58.56",
    "timezone": "America/Argentina/Buenos_Aires",
    "daily":['apparent_temperature_min','apparent_temperature_max',
              'temperature_2m_max', 'temperature_2m_min',
              'precipitation_probability_max','precipitation_hours',
              'precipitation_sum','showers_sum','rain_sum',
              'sunrise', 'sunset'],
    "start_date": '2022-12-01',
    "end_date":  '2023-05-16'
}

# Hacer la solicitud a la API
response = requests.get(url, params=params)

# Obtener los datos de la respuesta como un diccionario de Python
data_json = response.json()
cursor = conn.cursor()



# Preparar la consulta SQL de inserción
query = "INSERT INTO Fact_Historial_Clima (time,apparent_temperature_min, apparent_temperature_max,temperature_2m_max, temperature_2m_min, precipitation_probability_max, precipitation_hours, precipitation_sum, showers_sum,rain_sum,sunrise,sunset) VALUES (%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

# Recorrer los datos de data_json['daily'] y ejecutar la consulta para insertar cada fila en la tabla
for i in range(len(data_json['daily']['time'])):
    cursor.execute(query, (
        data_json['daily']['time'][i],
        data_json['daily']['apparent_temperature_min'][i],
        data_json['daily']['apparent_temperature_max'][i],
        data_json['daily']['temperature_2m_max'][i],
        data_json['daily']['temperature_2m_min'][i],
        data_json['daily']['precipitation_probability_max'][i],
        data_json['daily']['precipitation_hours'][i],
         data_json['daily']['precipitation_sum'][i],
         data_json['daily']['showers_sum'][i],
         data_json['daily']['rain_sum'][i],
         data_json['daily']['sunrise'][i],
         data_json['daily']['sunset'][i]
    ))

# Confirmar los cambios y cerrar la conexión
conn.commit()
cursor.close()
conn.close()