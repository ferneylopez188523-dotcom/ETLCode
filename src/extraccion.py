from pymongo import MongoClient
import os
from datetime import datetime
import logging
import pandas as pd
from pathlib import Path

class Extraccion:
    """
    Clase encargada de establecer la conexión a MongoDB y extraer las colecciones 
    Listings, Reviews y Calendar hacia DataFrames de pandas.
    """
    
    # Constructor de la clase, establece la conexión a MongoDB y selecciona la base de datos.
    def __init__(self, db_name="airbnb_buenos_aires", uri="mongodb://localhost:27017/"):
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None
        self._configurar_logs()
        
        
        
    def _configurar_logs(self):
        """
        Configura el sistema de logs creando un directorio 'logs' si no existe, 
        y generando un archivo .txt con un timestamp para almacenar los logs de la ejecución.
        """
        
        if not os.path.exists('logs'):
            os.makedirs('logs')

        fecha_hora = datetime.now().strftime("%Y%m%d_%H%M")
        log_filename = f"logs/log_{fecha_hora}.txt"
        
        # Configuración básica del logger.
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Opcional: Agregar salida por consola para ver en tiempo real durante el desarrollo
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logging.getLogger('').addHandler(console_handler)
    
    
    
    def conectar_mongodb(self):
        """
        Establece la conexión a MongoDB utilizando el URI proporcionado y selecciona la base de datos.
        Registra en los logs el éxito o cualquier error durante la conexión.
        """
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)  # Timeout de 5 segundos para la conexión
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            logging.info(f"Conexión a MongoDB establecida exitosamente, base de datos seleccionada: {self.db_name}")
            print(f"Conexión a MongoDB establecida exitosamente, base de datos seleccionada: {self.db_name}")
        except Exception as e:
            logging.error(f"Error al conectar a MongoDB, detalle: {e}")
            raise Exception(f"Error al conectar a MongoDB: {e}, verifique que el servidor esté corriendo y la URI sea correcta.")
    
    
    
    def extraer_datos(self) -> dict:
        """
        Consulta las 3 colecciones requeridas, carga los datos en DataFrames y registra las métricas.
        Retorna un diccionario con los DataFrames.
        """
        colecciones_mongo = ['Listings', 'Reviews', 'Calendar']
        dfs = {}

        if self.db is None:
            logging.error("Se intentó extraer datos sin conexión. Verifique que la conexión a MongoDB se haya establecido correctamente.")
            return dfs

        for col in colecciones_mongo:
            try:
                logging.info(f"Iniciando consulta de la colección: {col}")
                
                # Extracción desde MongoDB.
                cursor = self.db[col].find()
                df = pd.DataFrame(list(cursor)) # Convertimos el cursor a una lista y luego a un DataFrame
                
                if not df.empty:
                    # Limpieza inicial: omitir el '_id' nativo de MongoDB.
                    if '_id' in df.columns:
                        df = df.drop(columns=['_id'])
                    
                    dfs[col] = df
                    logging.info(f"Extracción completada. {len(df)} registros cargados exitosamente de '{col}'.")
                else:
                    logging.warning(f"La colección '{col}' no arrojó resultados o está vacía.")
                    dfs[col] = pd.DataFrame()
                    
            except Exception as e:
                logging.error(f"Error inesperado al consultar la colección '{col}': {e}")
        
        return dfs
    
# ==========================================
# Bloque de ejecución principal, testeo.
# ==========================================

if __name__ == "__main__":
    # 1. Instanciamos la clase de Extracción
    extractor = Extraccion(db_name="airbnb_buenos_aires")
    # 2. Conectamos a la BD
    extractor.conectar_mongodb()
    # 3. Extraemos los datos a nuestro diccionario de DataFrames.
    dataframes_extraidos = extractor.extraer_datos()
    
    # Validación e impresión por consola.
    print("\nResumen de DataFrames:")
    for nombre, df in dataframes_extraidos.items():
        print(f"- {nombre}: {df.shape[0]} filas, {df.shape[1]} columnas")