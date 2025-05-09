import pymongo
import pandas as pd
import os
import datetime

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "datos-historicos"

DATA_DIRECTORY = "stock_data"

client = None

try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    print(f"Conectado a MongoDB en '{MONGO_URI}', base de datos '{DB_NAME}'")

    processed_files_count = 0
    failed_files = []

    csv_files = [f for f in os.listdir(DATA_DIRECTORY) if f.endswith('.csv')]
    print(f"Encontrados {len(csv_files)} archivos CSV en '{DATA_DIRECTORY}'.")

    if not csv_files:
        print("No se encontraron archivos CSV en el directorio especificado. Asegúrate de que 'stock_data' exista y contenga tus archivos.")
        exit()

    for i, csv_file in enumerate(csv_files):
        ticker_name = csv_file.replace('.csv', '')

        collection_name = ticker_name.replace('.', '_').replace('$', '').replace(' ', '_')

        collection = db[collection_name]
        file_path = os.path.join(DATA_DIRECTORY, csv_file)

        print(f"[{i+1}/{len(csv_files)}] Procesando {csv_file} para la colección '{collection_name}'...")

        try:
            df = pd.read_csv(file_path, parse_dates=['Date'])

            if df.index.name == 'Date':
                df = df.reset_index()
                
            try:
                df['Date'] = pd.to_datetime(df['Date'], utc=True)

                if df['Date'].dt.tz is not None:
                    df['Date'] = df['Date'].dt.tz_localize(None)

            except Exception as date_processing_error:
                print(f"  Error al procesar fechas en {csv_file}: {date_processing_error}")
                print(f"  Saltando archivo '{csv_file}' debido a un problema con el formato de las fechas.")
                failed_files.append(csv_file)
                continue

            documents = df.to_dict(orient='records')

            if documents:
                result = collection.insert_many(documents)
                print(f"  Insertados {len(result.inserted_ids)} documentos en la colección '{collection_name}'.")
                processed_files_count += 1
            else:
                print(f"  Advertencia: El archivo '{csv_file}' no contiene datos para insertar.")
                failed_files.append(csv_file)

        except FileNotFoundError:
            print(f"  Error: El archivo '{file_path}' no fue encontrado. Saltando.")
            failed_files.append(csv_file)
        except pd.errors.EmptyDataError:
            print(f"  Error: El archivo '{csv_file}' está vacío. Saltando.")
            failed_files.append(csv_file)
        except Exception as e:
            print(f"  Error procesando '{csv_file}': {e}")
            failed_files.append(csv_file)

except pymongo.errors.ConnectionFailure as e:
    print(f"Error de conexión a MongoDB. Asegúrate de que el servidor esté ejecutándose en '{MONGO_URI}'. Detalle: {e}")
except Exception as e:
    print(f"Ha ocurrido un error inesperado en el script principal: {e}")
finally:
    if client:
        client.close()
        print("Conexión a MongoDB cerrada.")

print("\n--- Proceso de carga finalizado ---")
print(f"Total de archivos CSV encontrados: {len(csv_files)}")
print(f"Archivos procesados y cargados exitosamente a MongoDB: {processed_files_count}")
if failed_files:
    print(f"Archivos con errores o sin datos: {len(failed_files)}")
    print("Lista de archivos fallidos:", failed_files)