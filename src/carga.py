import logging
import os
import sqlite3
from datetime import datetime

import pandas as pd


class Carga:
    """
    Clase encargada de cargar los DataFrames transformados a SQLite y exportar a XLSX.
    """

    def __init__(self, dicc_dataframes: dict, db_path: str = "data/airbnb.db"):
        """
        Inicializa la clase con los DataFrames y la ruta de la base de datos.
        """
        self.dfs = dicc_dataframes
        self.db_path = db_path
        self._configurar_logs()
        self._crear_directorios()

    def _configurar_logs(self):
        """
        Configura el sistema de logs.
        """
        if not os.path.exists('logs'):
            os.makedirs('logs')
        
        fecha_hora = datetime.now().strftime("%Y%m%d_%H%M")
        log_filename = f"logs/log_{fecha_hora}.txt"
        
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        logging.getLogger('').addHandler(console)

    def _crear_directorios(self):
        """
        Crea los directorios necesarios para la base de datos.
        """
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

    def _crear_tabla_sqlite(self, conn: sqlite3.Connection, nombre: str, df: pd.DataFrame):
        """
        Crea una tabla en SQLite basada en el DataFrame.
        """
        columnas = list(df.columns)
        tipos = {
            'id': 'INTEGER PRIMARY KEY',
            'listing_id': 'INTEGER',
            'date': 'TEXT',
            'price': 'REAL',
            'available': 'TEXT',
            'year': 'INTEGER',
            'month': 'INTEGER',
            'day': 'INTEGER',
            'quarter': 'INTEGER',
            'año': 'INTEGER',
            'mes': 'INTEGER',
            'día': 'INTEGER',
            'trimestre': 'INTEGER',
        }
        
        columnas_sql = []
        for col in columnas:
            col_lower = col.lower()
            if col_lower in tipos:
                columnas_sql.append(f'"{col}" {tipos[col_lower]}')
            else:
                columnas_sql.append(f'"{col}" TEXT')
        
        columnas_def = ', '.join(columnas_sql)
        create_sql = f'CREATE TABLE IF NOT EXISTS "{nombre}" ({columnas_def})'
        conn.execute(create_sql)
        conn.commit()

    def cargar_sqlite(self) -> dict:
        """
        Inserta los DataFrames en la base de datos SQLite.
        Retorna un diccionario con el resultado de la carga por tabla.
        """
        logging.info("--- INICIANDO CARGA A SQLite ---")
        resultados = {}
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for nombre, df in self.dfs.items():
                if df.empty:
                    logging.warning(f"[{nombre}] DataFrame vacío, omitiendo.")
                    resultados[nombre] = {'estado': 'omitido', 'registros': 0}
                    continue
                
                try:
                    self._crear_tabla_sqlite(conn, nombre, df)
                    
                    df.to_sql(nombre, conn, if_exists='replace', index=False)
                    
                    cursor.execute(f'SELECT COUNT(*) FROM "{nombre}"')
                    count = cursor.fetchone()[0]
                    
                    resultados[nombre] = {'estado': 'exito', 'registros': count}
                    logging.info(f"[{nombre}] {count} registros insertados en SQLite.")
                except Exception as e:
                    resultados[nombre] = {'estado': 'error', 'mensaje': str(e)}
                    logging.error(f"[{nombre}] Error al insertar: {e}")
            
            conn.close()
            logging.info("--- CARGA A SQLite COMPLETADA ---")
        except Exception as e:
            logging.error(f"Error al conectar con SQLite: {e}")
        
        return resultados

    def exportar_xlsx(self, ruta: str = "data/exports", archivo: str = None) -> dict:
        """
        Exporta los DataFrames a uno o varios archivos XLSX.
        Si archivo es None, exporta cada DataFrame a un archivo separado.
        """
        logging.info("--- INICIANDO EXPORTACIÓN A XLSX ---")
        os.makedirs(ruta, exist_ok=True)
        resultados = {}
        """
        try:
            if archivo:
                ruta_completa = os.path.join(ruta, archivo)
                with pd.ExcelWriter(ruta_completa, engine='openpyxl') as writer:
                    
                    for nombre, df in self.dfs.items():
                        df.to_excel(writer, sheet_name=nombre, index=False)
                resultados[archivo] = {'estado': 'exito', 'hojas': len(self.dfs)}
                logging.info(f"Exportación múltiple a '{ruta_completa}' completada.")
            else:
                for nombre, df in self.dfs.items():
                    ruta_df = os.path.join(ruta, f"{nombre}.xlsx")
                    df.to_excel(ruta_df, index=False)
                    resultados[nombre] = {'estado': 'exito', 'ruta': ruta_df}
                    logging.info(f"[{nombre}] Exportado a '{ruta_df}'.")
            
            logging.info("--- EXPORTACIÓN A XLSX COMPLETADA ---")
        except Exception as e:
            logging.error(f"Error al exportar XLSX: {e}")
            resultados['error'] = str(e)"""
        LIMITE_FILAS_XLSX = 500000
            
        try:

            for nombre, df in self.dfs.items():

                filas = len(df)

                # Nombre base del archivo
                if archivo:
                    base = os.path.splitext(archivo)[0]
                    nombre_archivo = f"{base}_{nombre}"
                else:
                    nombre_archivo = nombre

                # -----------------------------------------
                # Si supera 500k filas -> CSV
                # -----------------------------------------
                if filas > LIMITE_FILAS_XLSX:

                    ruta_salida = os.path.join(ruta, f"{nombre_archivo}.csv")

                    df.to_csv(
                        ruta_salida,
                        index=False,
                        encoding="utf-8-sig"
                    )

                    resultados[nombre] = {
                        "estado": "exito",
                        "tipo": "csv",
                        "filas": filas,
                        "ruta": ruta_salida
                    }

                    logging.info(
                        f"[{nombre}] {filas} filas > {LIMITE_FILAS_XLSX}. Exportado CSV: {ruta_salida}"
                    )

                # -----------------------------------------
                # Si no supera 500k -> XLSX
                # -----------------------------------------
                else:

                    ruta_salida = os.path.join(ruta, f"{nombre_archivo}.xlsx")

                    with pd.ExcelWriter(ruta_salida, engine="openpyxl") as writer:
                        df.to_excel(writer, sheet_name=nombre[:31], index=False)

                    resultados[nombre] = {
                        "estado": "exito",
                        "tipo": "xlsx",
                        "filas": filas,
                        "ruta": ruta_salida
                    }

                    logging.info(
                        f"[{nombre}] {filas} filas <= {LIMITE_FILAS_XLSX}. Exportado XLSX: {ruta_salida}"
                    )

            logging.info("--- EXPORTACIÓN COMPLETADA ---")

        except Exception as e:
            logging.error(f"Error al exportar archivos: {e}")
            resultados["error"] = str(e)
        
        return resultados

    def verificar_carga(self, tabla: str) -> dict:
        """
        Verifica que los registros de una tabla se hayan cargado correctamente.
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(f'SELECT COUNT(*) FROM "{tabla}"')
            count = cursor.fetchone()[0]
            
            cursor.execute(f'PRAGMA table_info("{tabla}")')
            columnas = [row[1] for row in cursor.fetchall()]
            
            conn.close()
            
            return {
                'tabla': tabla,
                'registros': count,
                'columnas': columnas,
                'verificado': True
            }
        except Exception as e:
            logging.error(f"Error al verificar '{tabla}': {e}")
            return {'tabla': tabla, 'verificado': False, 'error': str(e)}

    def ejecutar_carga(self, exportar: bool = True, archivo_xlsx: str = None) -> dict:
        """
        Orquesta la ejecución completa de la carga.
        """
        logging.info("--- INICIANDO PROCESO DE CARGA ---")
        
        resultados_sqlite = self.cargar_sqlite()
        
        resultados_xlsx = {}
        if exportar:
            resultados_xlsx = self.exportar_xlsx(archivo=archivo_xlsx)
        
        logging.info("--- PROCESO DE CARGA COMPLETADO ---")
        
        return {
            'sqlite': resultados_sqlite,
            'xlsx': resultados_xlsx
        }




"""
if __name__ == "__main__":
    from extraccion import Extraccion
    from transformacion import Transformacion
    
    print("Extrayendo datos de MongoDB...")
    extractor = Extraccion()
    extractor.conectar_mongodb()
    datos_ = extractor.extraer_datos()
    
    print("Transformando datos...")
    transformador = Transformacion(datos_)
    datos_limpios = transformador.ejecutar_transformacion()
    
    print("Cargando datos...")
    cargador = Carga(datos_limpios)
    resultados = cargador.ejecutar_carga(exportar=True, archivo_xlsx="airbnb_completo.xlsx")
    
    print("\nResumen de carga:")
    for fuente, datos in resultados.items():
        print(f"\n{fuente.upper()}:")
        for tabla, info in datos.items():
            print(f"  {tabla}: {info}")
    
    for tabla in datos_limpios.keys():
        verificacion = cargador.verificar_carga(tabla)
        print(f"\nVerificacion {tabla}: {verificacion['registros']} registros cargados.")
"""
