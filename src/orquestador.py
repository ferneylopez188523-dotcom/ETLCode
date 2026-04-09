import logging
import os
from datetime import datetime

from carga import Carga

# Importamos nuestras clases desde la carpeta src
from extraccion import Extraccion
from transformacion import Transformacion


def configurar_log_orquestador():
    """Configura un log general para monitorear el pipeline completo."""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    fecha_hora = datetime.now().strftime("%Y%m%d_%H%M")
    log_filename = f"logs/etl_main_{fecha_hora}.txt"
    
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - [MAIN] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    
    # Evitamos duplicar logs en consola si ya hay otros handlers
    if not logging.getLogger('').handlers:
        logging.getLogger('').addHandler(console)


def ejecutar_etl():
    """Función principal que orquesta la Extracción, Transformación y Carga."""
    configurar_log_orquestador()
    logging.info("========================================")
    logging.info("INICIANDO PIPELINE ETL - AIRBNB")
    logging.info("========================================")

    try:
        # -----------------------------------------
        # FASE 1: EXTRACCIÓN
        # -----------------------------------------
        logging.info(">>> FASE 1: INICIANDO EXTRACCIÓN")
        extractor = Extraccion()
        extractor.conectar_mongodb()
        datos_ = extractor.extraer_datos()
        
        if not datos_:
            logging.error("La extracción falló o no devolvió datos. Se detiene el pipeline.")
            return

        # -----------------------------------------
        # FASE 2: TRANSFORMACIÓN
        # -----------------------------------------
        logging.info(">>> FASE 2: INICIANDO TRANSFORMACIÓN")
        transformador = Transformacion(datos_)
        datos_limpios = transformador.ejecutar_transformacion()

        if not datos_limpios:
            logging.error("La transformación falló. Se detiene el pipeline.")
            return

        # -----------------------------------------
        # FASE 3: CARGA (Próximo paso del taller)
        # -----------------------------------------
        logging.info(">>> FASE 3: INICIANDO CARGA (Pendiente de implementar)")

        """Acá irá el llamado de la clase de carga, 
        que se encargará de tomar los DataFrames limpios y 
        cargarlos a la base de datos destino (SQLite)."""

        # Después de la transformación:
        logging.info(">>> FASE 3: INICIANDO CARGA")
        cargador = Carga(datos_limpios)
        resultados = cargador.ejecutar_carga(exportar=True)
        logging.info(f"Resultado de carga: {resultados}")


        logging.info("========================================")
        logging.info("PIPELINE ETL FINALIZADO CON ÉXITO")
        logging.info("========================================")

    except Exception as e:
        logging.critical(f"EL PIPELINE HA FALLADO CRÍTICAMENTE: {e}")

if __name__ == "__main__":
    ejecutar_etl()