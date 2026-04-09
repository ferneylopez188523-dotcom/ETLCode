import json
import logging
import os
from datetime import datetime

import numpy as np
import pandas as pd


class Transformacion:
    """
    Clase encargada de transformar y limpiar los DataFrames extraídos.
    Implementa normalización de tipos de datos, limpieza de nulos/duplicados,
    derivación de variables temporales y tratamiento de texto.
    """

    def __init__(self, dicc_dataframes: dict):
        """
        Inicializa la clase recibiendo el diccionario de DataFrames de la fase de extracción.
        """
        self.dfs = dicc_dataframes.copy()
        self.dfs_limpios = {}
        self._configurar_logs()
        

    def _configurar_logs(self):
        """
        Configuramos el sistema de logs .
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
        # Impresión por consola
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        logging.getLogger('').addHandler(console)

    def _limpiar_basicos(self, df: pd.DataFrame, nombre_df: str, claves: list[str]) -> pd.DataFrame:
        """
        Elimina registros duplicados basados en las claves proporcionadas y gestiona valores nulos.
        """
        registros_antes = len(df)
        
        claves_existentes = [c for c in claves if c in df.columns]
        if claves_existentes:
            df = df.drop_duplicates(subset=claves_existentes)
            df = df.dropna(subset=claves_existentes)
        else:
            df = df.drop_duplicates()
            
        registros_despues = len(df)
        eliminados = registros_antes - registros_despues
        logging.info(f"[{nombre_df}] Limpieza básica: {registros_antes} -> {registros_despues} registros. ({eliminados} eliminados).")
        return df

    def _normalizar_precio(self, df: pd.DataFrame, columna: str) -> pd.DataFrame:
        """
        Convierte campos de precio (String con '$' y ',') a valores numéricos (Float).
        """
        if columna in df.columns:
            try:
                # Solución al SyntaxWarning: Usamos r'[$,]' sin la barra invertida
                df[columna] = df[columna].astype(str).replace(r'[$,]', '', regex=True)
                df[columna] = pd.to_numeric(df[columna], errors='coerce')
                logging.info(f"Columna '{columna}' normalizada a formato numérico.")
            except Exception as e:
                logging.error(f"Error al normalizar precio en '{columna}': {e}")
        return df
        

    def _procesar_fechas(self, df: pd.DataFrame, columna: str) -> pd.DataFrame:
        """
        Convierte fechas a formato YYYY-MM-DD y deriva año, mes, día y trimestre.
        """
        if columna in df.columns:
            try:
                df[columna] = pd.to_datetime(df[columna], errors='coerce')
                
                # Derivaciones temporales.
                df['año'] = df[columna].dt.year
                df['mes'] = df[columna].dt.month
                df['día'] = df[columna].dt.day
                df['trimestre'] = df[columna].dt.quarter
                
                # Estandarizamos al formato string YYYY-MM-DD.
                df[columna] = df[columna].dt.strftime('%Y-%m-%d')
                logging.info(f"Fechas estandarizadas y variables derivadas (año, mes, día, trimestre) creadas a partir de '{columna}'.")
            except Exception as e:
                logging.error(f"Error al procesar fechas en '{columna}': {e}")
        return df

    def _categorizar_precios(self, df: pd.DataFrame, columna: str) -> pd.DataFrame:
        """
        Categoriza los precios numéricos en rangos (Económico/Estándar/Alto/Premium)
        dentro de cada room_type para evitar distorsiones.
        """
        if columna not in df.columns or 'room_type' not in df.columns:
            return df

        if not pd.api.types.is_numeric_dtype(df[columna]):
            return df

        try:
            df = df.copy()
            etiquetas = ['Económico', 'Estándar', 'Alto', 'Premium']
            df[f'{columna}_rango'] = None

            for rt in df['room_type'].unique():
                mask = df['room_type'] == rt
                datos_rt = df.loc[mask, columna].dropna()

                if len(datos_rt) < 4:
                    df.loc[mask, f'{columna}_rango'] = pd.cut(
                        datos_rt, bins=3, labels=etiquetas[:len(datos_rt)], duplicates='drop'
                    )
                else:
                    df.loc[mask, f'{columna}_rango'] = pd.qcut(
                        datos_rt, q=4, labels=etiquetas, duplicates='drop'
                    )

            df[f'{columna}_rango'] = df[f'{columna}_rango'].cat.add_categories([None])
            logging.info(f"Precios categorizados por room_type en '{columna}_rango'.")
        except Exception as e:
            logging.warning(f"No se pudo categorizar '{columna}' por room_type: {e}")
        return df

    def _desanidar_texto(self, df: pd.DataFrame, columna: str) -> pd.DataFrame:
        """
        Limpia caracteres de listas o JSONs anidados en strings (ej. amenities).
        """
        if columna in df.columns:
            try:
                # Quita llaves, corchetes y comillas para dejar un texto limpio separado por comas
                df[columna] = df[columna].astype(str).replace(r'[{}\[\]"´`]', '', regex=True)
                logging.info(f"Campo complejo '{columna}' desanidado y limpiado.")
            except Exception as e:
                logging.error(f"Error al desanidar '{columna}': {e}")
        return df
    
    def flatten_columna(self, df: pd.DataFrame, columna: str) -> pd.DataFrame:
        """
        Si la columna contiene listas o JSONs, la convierte a formato de lista real y luego hace explode para aplanar.
        """
        if columna in df.columns:
            # Convertir string a lista real
            df[columna] = df[columna].apply(
                lambda x: json.loads(x) if isinstance(x, str) else x
            )
            
            # Explode (flatten)
            df = df.explode(columna)
        
        return df


    def ejecutar_transformacion(self) -> dict:
        """
        Orquesta la ejecución de todas las transformaciones por cada DataFrame.
        Retorna el diccionario de DataFrames.
        """
        logging.info("--- INICIANDO PROCESO DE TRANSFORMACIÓN ---")
        
        for nombre, df in self.dfs.items():
            if df.empty:
                logging.warning(f"El DataFrame '{nombre}' está vacío.....")
                continue
                
            logging.info(f"Transformando tabla: {nombre}...")
            df_t = df.copy()
            
            CLAVES_DUPLICADOS = {
    "Listings":  ["id"],
    "Reviews":   ["id"],
    "Calendar":  ["listing_id", "date"],
}
            CLAVES_DUPLICADOS = {
                "Listings":  ["id"],
                "Reviews":   ["id"],
                "Calendar":  ["listing_id", "date"],
            }
                        # 1. Limpieza.
            claves = CLAVES_DUPLICADOS.get(nombre, [])
            df_t = self._limpiar_basicos(df_t, nombre, claves)
            
            # 2. Transformaciones específicas.
            if nombre == 'Listings':
                #df_t = self._normalizar_precio(df_t, 'price')
                df_t = self._categorizar_precios(df_t, 'price')
                df_t = self._desanidar_texto(df_t, 'amenities')
                df_t = self._desanidar_texto(df_t, 'host_verifications')
                
            elif nombre == 'Calendar':
                df_t = self._normalizar_precio(df_t, 'price')
                df_t = self._procesar_fechas(df_t, 'date')
                
            elif nombre == 'Reviews':
                df_t = self._procesar_fechas(df_t, 'date')
                
            self.dfs_limpios[nombre] = df_t
            logging.info(f"Transformación de '{nombre}' finalizada con éxito.")
            
        logging.info("--- PROCESO DE TRANSFORMACIÓN COMPLETADO ---")
        return self.dfs_limpios

# ==========================================
# Bloque de ejecución principal, testeo.
# ==========================================
""""if __name__ == "__main__":
    # Importamos la extracción para simular el flujo
    from extraccion import Extraccion
    
    print("Extrayendo datos de MongoDB...")
    extractor = Extraccion()
    extractor.conectar_mongodb()
    datos_ = extractor.extraer_datos()
    
    print("Iniciando transformaciones...")
    transformador = Transformacion(datos_)
    datos_limpios = transformador.ejecutar_transformacion()
    
    print("\nResumen Post-Transformación:")
    for nombre, df in datos_limpios.items():
        print(f"Tabla {nombre}: {df.shape[0]} registros listos.")
        if nombre == 'Listings' and 'price_rango' in df.columns:
            print(f" -> Muestra categorización: \n{df[['price', 'price_rango']].head(3)}")"""