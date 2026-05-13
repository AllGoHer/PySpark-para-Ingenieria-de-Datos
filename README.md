# PySpark-para-Ingenieria-de-Datos
______________________________________________________________________________________________________________________________________________________________________________________________________________________________
En el recorrido de PySpark para ingenieros de datos, veremos:

• Cómo funciona Spark internamente (arquitectura y modelo de ejecución)
• Cómo trabajar con DataFrames, esquemas y Spark SQL
• Cómo leer y escribir datos a gran escala (CSV, JSON, Parquet)
• Transformaciones, acciones y evaluación diferida
• Manejo de datos desordenados y del mundo real en Spark
• Uniones, agregaciones, funciones de ventana y análisis
• Ajuste del rendimiento, partición y optimización
• Escritura de pipelines de PySpark listos para producción
• Cómo encaja PySpark en los sistemas reales de ingeniería de datos
______________________________________________________________________________________________________________________________________________________________________________________________________________________________

#**PySpark**
## Enlace con Docker
Veremos como trabaja PySpark internamente mediante el uso de Docker Desktop y la interface de usuario de Spark (http://localhost:4040).

1.	Iniciaremos creando una carpeta agrupada con Docker Compose, para lograr la siguiente carga en Docker desktop:

    Paso 1: Crear la estructura de carpetas

En tu terminal de VS Code, ve a la carpeta donde estás trabajando (ej: C:\Users\User\PySpark).

Crea una carpeta nueva para este proyecto:
bash

mkdir spark-docker

cd spark-docker

Paso 2: Crear el archivo "Receta" (docker-compose.yml)

Dentro de la carpeta spark-docker, vas a crear un archivo llamado exactamente docker-compose.yml (sin espacios, todo en minúsculas).

Copia y pega el siguiente código dentro de ese archivo:

Yaml:
      version: "3.8"

      services:
  
  spark-master:
    
    image: apache/spark:3.5.1
    container_name: spark-master
    hostname: spark-master
    command: >
      bash -c "
      /opt/spark/sbin/start-master.sh &&
      tail -f /opt/spark/logs/spark--org.apache.spark.deploy.master*.out
      "
    ports:
      - "8080:8080"   # Spark Master UI
      - "7077:7077"   # Spark Master port
    volumes:
      - ./apps:/opt/spark-apps
      - ./data:/opt/spark-data

  spark-worker-1:
    image: apache/spark:3.5.1
    container_name: spark-worker-1
    hostname: spark-worker-1
    command: >
      bash -c "/opt/spark/sbin/start-worker.sh spark://spark-master:7077 --cores 2 --memory 2g &&
      tail -f /opt/spark/logs/spark--org.apache.spark.deploy.worker*.out"
    ports:
      - "8081:8081"
      - "4040:4040"
    depends_on:
      - spark-master
    volumes:
      - ./apps:/opt/spark-apps
      - ./data:/opt/spark-data

  spark-worker-2:
    image: apache/spark:3.5.1
    container_name: spark-worker-2
    hostname: spark-worker-2
    command: >
      bash -c "/opt/spark/sbin/start-worker.sh spark://spark-master:7077 --cores 2 --memory 2g &&
      tail -f /opt/spark/logs/spark--org.apache.spark.deploy.worker*.out"
    ports:
      - "8082:8081"
    depends_on:
      - spark-master
    volumes:
      - ./apps:/opt/spark-apps
      - ./data:/opt/spark-data
