import datetime
import logging
import os
import requests
import pyodbc
import azure.functions as func

app = func.FunctionApp()

# Se define la función con un decorador de temporizador
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="mytimer", run_on_startup=False) 
def WeatherCollector(mytimer: func.TimerRequest) -> None:
    logging.info('Python Timer trigger function started at %s', datetime.datetime.now().isoformat())

    try:
        # 1. Obtener datos de la API del tiempo
        api_key = os.environ["WEATHER_API_KEY"]
        api_url_template = os.environ["WEATHER_API_URL"]
        
        api_url = api_url_template.format(api_key)
        
        response = requests.get(api_url)
        response.raise_for_status()
        
        weather_data = response.json()
        
        # Extraer los datos relevantes (ejemplo con OpenWeatherMap)
        city = weather_data['name']
        temperature = weather_data['main']['temp']
        humidity = weather_data['main']['humidity']
        description = weather_data['weather'][0]['description']
        timestamp = datetime.datetime.now()
        
        logging.info(f"Datos del clima obtenidos para {city}: {temperature}°C")

        # 2. Conectarse a la base de datos y guardar los datos
        sql_conn_string = os.environ["SQL_CONNECTION_STRING"]
        cnxn = pyodbc.connect(sql_conn_string)
        cursor = cnxn.cursor()

        sql_insert = """
        INSERT INTO WeatherData (City, Temperature, Humidity, Description, Timestamp)
        VALUES (?, ?, ?, ?, ?);
        """
        cursor.execute(sql_insert, city, temperature, humidity, description, timestamp)
        cnxn.commit()
        
        logging.info("Datos guardados con éxito en Azure SQL.")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error al conectar con la API del tiempo: {e}")
    except pyodbc.Error as e:
        sql_error_message = f"Error al interactuar con la base de datos: {e}"
        logging.error(sql_error_message)
    except KeyError as e:
        logging.error(f"Error al analizar el JSON: Falta la clave {e}")
    except Exception as e:
        logging.error(f"Ocurrió un error inesperado: {e}")