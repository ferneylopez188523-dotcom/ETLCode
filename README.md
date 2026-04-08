# ETL Airbnb Buenos Aires

Proceso ETL (Extracción, Transformación, Carga) automatizado para los datasets de Airbnb de Ciudad Autónoma de Buenos Aires, Argentina. Desarrollado en Python con MongoDB como fuente de datos.

## Estructura del Proyecto

```
etl_airbnb/
├── src/
│   ├── extraccion.py      # Clase Extraccion (conexión MongoDB)
│   ├── transformacion.py  # Clase Transformacion (limpieza/normalización)
│   └── orquestador.py     # Orquestador del pipeline ETL
├── notebooks/
│   └── exp_airbnb.ipynb   # Análisis Exploratorio de Datos (EDA)
├── logs/                  # Archivos de log generados
├── requirements.txt       # Dependencias del proyecto
└── README.md
```

## Requisitos

- Python 3.8+
- MongoDB (local o remoto)
- Las colecciones `Listings`, `Reviews` y `Calendar` en la base de datos `airbnb_buenos_aires`

## Instalación

1. **Crear entorno virtual:**

```bash
python -m venv venv
```

2. **Activar entorno virtual:**
   - Windows: `venv\Scripts\activate`
   - Linux/Mac: `source venv/bin/activate`

3. **Instalar dependencias:**

```bash
pip install -r requirements.txt
```

4. **Configurar MongoDB:**
   - Asegúrate de que MongoDB esté corriendo localmente en el puerto 27017
   - Importa los archivos CSV (`listings.csv.gz`, `reviews.csv.gz`, `calendar.csv.gz`) como colecciones

## Uso

### Ejecutar Pipeline ETL Completo

```bash
cd src
python orquestador.py
```

### Ejecutar solo Extracción

```python
from extraccion import Extraccion

extractor = Extraccion()
extractor.conectar_mongodb()
datos = extractor.extraer_datos()
```

### Ejecutar solo Transformación

```python
from transformacion import Transformacion

transformador = Transformacion(datos)
datos_limpios = transformador.ejecutar_transformacion()
```

### Análisis Exploratorio (EDA)

```bash
jupyter notebook notebooks/exp_airbnb.ipynb
```

## Componentes

### Extraccion

- Conexión a MongoDB
- Extracción de colecciones a DataFrames pandas
- Registro de logs por colección

### Transformacion

- Limpieza de nulos y duplicados
- Normalización de precios (formato monetario a numérico)
- Conversión de fechas a formato YYYY-MM-DD
- Derivación de variables temporales (año, mes, día, trimestre)
- Categorización de precios
- Desanidado de campos complejos (amenities)

### Orquestador

- Coordina las fases ETL
- Manejo centralizado de logs

## Logs

Los archivos de log se generan automáticamente en la carpeta `logs/` con el formato:

```
logs/log_YYYYMMDD_HHMM.txt
logs/etl_main_YYYYMMDD_HHMM.txt
```

## Integrantes

| Rol | Responsabilidad               |
| --- | ----------------------------- |
| -   | Extracción y conexión MongoDB |
| -   | Transformación de datos       |
| -   | Análisis EDA                  |
| -   | Documentación                 |

## Licencia

Proyecto académico - Inteligencia de Negocios
