# Coderhouse

#La idea del proyecto es generar un historial de clima y un forecasting del clima, el historial va a servir para entrenar modelos de machine learning, mientras que
el forecasting servirá para predecir la tasa de ausentismo en un hospital. Si bien varias caracteristicas más son necesarias para este modelo, una variable necesaria es el clima, ya que intuyo que es relevante para el modelo.

Los datos se actualizan a diario, la idea del código es que el historial quede intacto (ya que no debería haber ningún dato a actualizar), mientras que los datos de forescasting se actualizan a diario, ya sea porque es una fecha más proxima y el forecasting será más acertado o ya sea porque la fecha ya pasó y es necesario actualizar los datos con datos reales.

Además, se creó una alerta para informar si la base por alguna razón no se está actualizando.

Para iniciar el proyecto solo basta con descargar el código, levantar el docker con <docker compose up> y listo, los dags se levantarán automáticamente junto con config.ini, aquí se encuentran las credenciales, es importante que el formato de este archivo se contenga y debe ser el siguiente:

[Email]
username = <username>
password = <password>

[redshift_coder]
dbname = <base de datos>
user = <usuario>
password = <contraseña>
host = <host>
port = <puerto>
