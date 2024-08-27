import os
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
import yfinance as yf
import pyodbc
import csv

#directorios para guardar los logs

log_dir = './logs'
data_dir = './data'

#si no existe crea los directorios log

if not os.path.exists(log_dir):
	os.makedirs(log_dir)
if not os.path.exists(data_dir):
	os.makedirs(data_dir)

#Configuración del logging

log_filename = os.path.join(log_dir, 'etl_process.log')

logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(levelname)s - %(message)s',
	handlers=[
		logging.FileHandler(log_filename),
		logging.StreamHandler()
	]
)

#Obtener listado S&P500

def extract_data():
	try:
		logging.info(f'Extrayendo listado S&P500')
		url = "https://es.wikipedia.org/wiki/Anexo:Compa%C3%B1%C3%ADas_del_S%26P_500"
		data = pd.read_html(url)
		data = data[0]
		logging.info(f'listado S&P500 extraído exitosamente')
		return data
	except Exception as e:
		logging.error(f'Error extrayendo listado S&P500 de Wikipedia: {e}')
		return None

def transform_data(data):
	try:
		logging.info('Transformando datos')
		df = data[['Símbolo', 'Seguridad', 'Sector GICS', 'Ubicación de la sede', 'Fundada']]
		df.rename(columns={'Símbolo': 'Symbol', 'Seguridad': 'Company', 'Sector GICS': 'Sector', 'Ubicación de la sede': 'Headquarters', 'Fundada': 'Established'}, inplace=True)
		logging.info('Datos transformados exitosamente')
		return df
	except Exception as e:
		logging.error(f'Error transformando datos: {e}')
		return None

def load_data(df):
    try:
        filename = os.path.join(data_dir, f'S&P500_list.csv')
        logging.info(f'Guardando datos transformados en {filename}')
        df.to_csv(filename, index=False)
        logging.info('Datos guardados exitosamente')
    except Exception as e:
        logging.error(f'Error guardando datos: {e}')

def etl_process():
    data = extract_data()
    if data is not None:
      transformed_data = transform_data(data)
      if transformed_data is not None:
        load_data(transformed_data)
        return transformed_data
    return None

data = etl_process()

#Obtener precios S&P500

def extract_data(data, start_date, end_date):
	#tickers = ['MMM', 'AOS', 'ABT']
	tickers = data["Symbol"]
	df_list = []
	for ticker in tickers:
		try:
			logging.info(f'Extrayendo precios {ticker}')
			price = yf.download(ticker, group_by="Ticker", start=start_date, end=end_date)
			price['Ticker'] = ticker  
			df_list.append(price)
			logging.info(f'precio extraído exitosamente')
		except Exception as e:
			logging.error(f'Error extrayendo precio para {ticker}: {e}')
			return None			
	data = pd.concat(df_list)
	return data

def transform_data(data):
	try:
		logging.info('Transformando datos')
		df = data[['Close', 'Ticker']].reset_index()
		df['Close'] = df['Close'].astype('float64')
		logging.info('Datos transformados exitosamente')
		return df
	except Exception as e:
		logging.error(f'Error transformando datos: {e}')
		return None

def load_data(df):
    try:
        filename = os.path.join(data_dir, f'S&P500_prices.csv')
        logging.info(f'Guardando datos transformados en {filename}')
        df.to_csv(filename, index=False)
        logging.info('Datos guardados exitosamente')
    except Exception as e:
        logging.error(f'Error guardando datos: {e}')

def etl_process(data, start_date, end_date):
    data = extract_data(data, start_date, end_date)
    if data is not None:
      transformed_data = transform_data(data)
      if transformed_data is not None:
        load_data(transformed_data)
        return transformed_data
    return None

start_date = '2024-01-01'
end_date = '2024-03-31'
data = etl_process(data, start_date, end_date)

# Configuración de la conexión
server = 'DESKTOP-1SPST8V\SQLEXPRESS'
database = 'SP500_DB'
username = 'imantill'
password = 'Imantilla71'
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Conexión a la base de datos
conn = pyodbc.connect(connection_string)
cursor = conn.cursor()
print("Conectado a SP500_DB")

# Leer el archivo CSV y cargar en tabla CompanyProfile

filename = os.path.join(data_dir, f'S&P500_list.csv')

cursor.execute("TRUNCATE TABLE CompanyProfiles;")

with open(filename, 'r') as file:
    csv_reader = csv.reader(file)
    
    # Leer encabezados (nombres de columnas)
    headers = ("Symbol","Company","Sector","Headquarters","Established",)
    
    # Crear la consulta de inserción dinámica
    placeholders = ', '.join(['?'] * len(headers))
    insert_query = f'INSERT INTO CompanyProfiles ({", ".join(headers)}) VALUES ({placeholders})'  # Cambia el nombre de la tabla
    
    # Insertar cada fila en la base de datos
    next(csv_reader)
    for row in csv_reader:
        cursor.execute(insert_query, row)

# Leer el archivo CSV y cargar en tabla Companies

filename = os.path.join(data_dir, f'S&P500_prices.csv')

cursor.execute("TRUNCATE TABLE Companies;")

with open(filename, 'r') as file:
    csv_reader = csv.reader(file)
    
    # Leer encabezados (nombres de columnas)
    headers = ("Date","ClosePrice","Symbol")
    
    # Crear la consulta de inserción dinámica
    placeholders = ', '.join(['?'] * len(headers))
    insert_query = f'INSERT INTO Companies ({", ".join(headers)}) VALUES ({placeholders})'  # Cambia el nombre de la tabla
    
    # Insertar cada fila en la base de datos
    next(csv_reader)
    for row in csv_reader:
          cursor.execute(insert_query, row)

conn.commit()

# Consultar
query = 'SELECT * FROM Companies'
cursor.execute(query)
records = cursor.fetchall()
print("Total rows are:  ", len(records))


# Cerrar la conexión
cursor.close()
conn.close()