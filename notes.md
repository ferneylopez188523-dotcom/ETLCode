# Taller 02: Proceso ETL con los datasets de Airbnb Ciudad Autónoma de Buenos Aires, Argentina (20%)

## Checklist

Todo el punto 1. Conexión y Extracción y el todo el punto 2. EDA están listos. Esta es la estructura del proyecto, el archivo transformacion.py que es el punto 3 está casi al 100%, falta cosas pequeñas como agregar trimestre y verificar que todos los puntos se encuentren hechos, por si se me pasó.
El archivo orquestador.py ejecuta extraccion.py y transformacion.py, si lo van a ejecutar desde consola sería python src/orquestador.py para que guarde los logs en la carpeta logs, si se ejecuta dentro de src (python orquestador.py crea una carpeta logs internamente, eso no lo logré solucionar. Entonces punto 1, 2 terminados 100% el punto 3 está creería que un 95%, quedó en el orquestador.py la sección para hacer el call a la clase Carga. Revisen porfa y si ven algo extraño me cuentan o si me hizo falta algo, se los agradecería.

Nota: No sé si le pase a ustedes también pero al cargar el archivo de Listings a MongoDB no me carga la columna 'price' (aunque está totalmente vacía), me cuentan cualquier cosa.
