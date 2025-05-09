import yfinance as yf
import pandas as pd
import os
import time

tickers_file = 'tickers.txt'
start_date = '2005-01-01'
end_date = '2025-01-01'
output_directory = 'stock_data'

if not os.path.exists(output_directory):
    os.makedirs(output_directory)
    print(f"Directorio '{output_directory}' creado.")

tickers = []
try:
    with open(tickers_file, 'r') as f:
        for line in f:
            ticker = line.strip()
            if ticker and not ticker.startswith('#'):
                tickers.append(ticker)
except FileNotFoundError:
    print(f"Error: El archivo '{tickers_file}' no fue encontrado.")
    exit()

print(f"Se han leído {len(tickers)} tickers del archivo '{tickers_file}'.")

downloaded_count = 0
failed_tickers = []
no_data_in_range_tickers = []

for i, ticker in enumerate(tickers):
    print(f"[{i+1}/{len(tickers)}] Procesando ticker: {ticker}...")

    safe_ticker = ticker.replace('.', '_').replace('-', '_').replace('^', '')
    output_filename = os.path.join(output_directory, f"{safe_ticker}.csv")

    data = pd.DataFrame() 

    try:
        print(f"  Intentando descargar datos de {start_date} a {end_date}...")
        data = yf.Ticker(ticker).history(start=start_date, end=end_date)

        if data.empty:
            print(f"  No se encontraron datos para {ticker} en el rango {start_date} a {end_date}.")
            no_data_in_range_tickers.append(ticker)

            print(f"  Intentando descargar todo el historial disponible para {ticker}...")
            full_data = yf.Ticker(ticker).history()

            if not full_data.empty:
                print(f"  Se encontró historial completo para {ticker}. Guardando todos los datos disponibles.")
                data = full_data
            else:
                print(f"  Advertencia: No se encontró historial para {ticker} en ningún rango.")
                failed_tickers.append(ticker)

    except Exception as e:
        print(f"  Error al intentar descargar datos para {ticker}: {e}")
        failed_tickers.append(ticker)
        data = pd.DataFrame()

    if not data.empty:
        try:
            data.to_csv(output_filename)
            print(f"  Datos guardados para {ticker} en {output_filename}")
            downloaded_count += 1
        except Exception as e:
             print(f"  Error al guardar el archivo para {ticker}: {e}")
             pass

print("\n--- Proceso finalizado ---")
print(f"Total de tickers procesados: {len(tickers)}")
print(f"Tickers con datos descargados exitosamente (al menos algo de historial): {downloaded_count}")
if no_data_in_range_tickers:
    print(f"Tickers que no tuvieron datos en el rango {start_date} a {end_date}: {len(no_data_in_range_tickers)}")

if failed_tickers:
    print(f"Tickers que fallaron completamente (sin historial encontrado o error): {len(failed_tickers)}")
    print("Lista:", failed_tickers)