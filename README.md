# PySpark-para-Ingenieria-de-Datos
_____________________________________________________________________________________________________________________________________________________________________________________________________________________________
En el recorrido de PySpark para ingenieros de datos, veremos:

• Cómo funciona Spark internamente (arquitectura y modelo de ejecución)

• Cómo trabajar con DataFrames, esquemas y Spark SQL.

• Cómo leer y escribir datos a gran escala (CSV, JSON, Parquet).

• Transformaciones, acciones y evaluación diferida.

• Manejo de datos desordenados y del mundo real en Spark.

• Uniones, agregaciones, funciones de ventana y análisis.

• Ajuste del rendimiento, partición y optimización.

• Escritura de pipelines de PySpark listos para producción.

• Cómo encaja PySpark en los sistemas reales de ingeniería de datos.
_____________________________________________________________________________________________________________________________________________________________________________________________________________________________

# **PySpark**
## Enlace con Docker
Veremos como trabaja PySpark internamente mediante el uso de Docker Desktop y la interface de usuario de Spark (http://localhost:4040).

1.	Iniciaremos creando una carpeta agrupada con Docker Compose, para lograr la siguiente carga en Docker desktop:

    Paso 1: Crear la estructura de carpetas

En tu terminal de VS Code, ve a la carpeta donde estás trabajando (ej: C:\Users\User\PySpark).

Crea una carpeta nueva para este proyecto:

Bash:
     
      mkdir spark-docker


Bash:
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



¿Qué está pasando en este código que acabas de pegar?

services: Le dice a Docker que vamos a crear 3 "servicios" (contenedores).

image: apache/spark:3.5.1 Es la imagen oficial que descarga de internet.

depends_on: Es crucial. Le dice a los workers: "No se enciendan hasta que spark-master esté totalmente encendido".

SPARK_MASTER_URL=spark://spark-master:7077 Es la magia de Docker. Los workers necesitan saber dónde está el jefe (el master). Como están en la misma red interna de Docker, pueden llamarse por su nombre de contenedor (spark-master).

**Paso 3: ¡Ejecutar la magia!**

Ahora, asegúrate de que tu terminal esté dentro de la carpeta spark-docker (la ruta debería decir algo como (venv) PS C:\Users\User\PySpark\spark-docker>).

Ejecuta este comando:

bash

docker-compose up -d

(El -d es para que corra en segundo plano).

**Paso 4: Verificar en Docker Desktop**

Ve a tu Docker Desktop y haz clic en la pestaña "Containers" (o "Containers / Apps").


Veras la carpeta agrupadora llamada spark-docker con la flechita hacia abajo.

Adentro, spark-master con la imagen apache/spark:3.5.1 y el puerto 7077:7077.

Abajo, spark-worker-1 con su puerto 4040.

Abajo, spark-worker-2 con su puerto 8082:8081.

![image](https://github.com/user-attachments/assets/cc49043e-7ad2-491d-8437-12fb17d3a490)


Ahora El Ejecutor, ve a bash o tu terminal y ejecuta el siguiente comando:
    
docker exec -it spark-worker-1 /opt/spark/bin/pyspark --master spark://spark-master:7077

Verás que aparece la consola de PySpark (>>>).

![image](https://github.com/user-attachments/assets/846efe36-6363-423a-b58c-c6216347a9ac)

Ahora para verificar vamos al localhost:4040 en tu navegador.

![image](https://github.com/user-attachments/assets/0103e938-c779-4b21-b5f9-4ff1d3b82c20)

Ahora hagamos unos ejercicios para ver como funciona 

>>> data = [(i % 10, i) for i in range(1_000_000)]
>>> df = spark.createDataFrame(data, ['key', 'value'])
>>> df.show()

Se mostrará lo siguiente:

![image](https://github.com/user-attachments/assets/e513f729-38ff-4211-b3d2-276696985873)

>>> df.count()
1000000
>>> data = [(i, i * 10) for i in range(1_000_000)]
>>> df = spark.createDataFrame(data, ['id', 'value'])
>>> from pyspark.sql.functions import col
>>> df_filtered = df.filter(col('value') > 5000)
>>> df_select = df_filtered.select('id')
>>> df_select.show()

•	ver las acciones o trabajo en puerto desde el navegador 

![image](https://github.com/user-attachments/assets/c8123ce1-dd1f-423b-bef5-82c670785a65)

3.	Gráficos Acíclicos Directos (DAGs)

3.1 Práctica: Revisión de DAG lógicos y físicos
Objetivo de la práctica
Aprender a:
•    Revisar planes lógicos
•    Revisar planes físicos
•    Identificar reordenaciones antes de la ejecución

Paso 1: Crear un conjunto de datos

>>> from pyspark.sql import SparkSession
>>> from pyspark.sql.functions import col

>>> spark = SparkSession.builder.getOrCreate()

>>> data = [(i % 5, i) for i in range(1_000_000)]

>>> df = spark.createDataFrame(data, ["category", "value"])

•	Step 2: Define Transformaciones (No Ejecución)

>>> result_df = (
       df.filter(col("value") > 1000)
       .groupBy("category")
       .count()
)

•	Step 3: Inspección del plan físico 
              >>> result_df.explain(mode="formatted")

Observa:
•    Operador de intercambio
•    Agregación basada en hash
•    Límites de etapa implícitos por la barajadura
Resultados en terminal:

![image](https://github.com/user-attachments/assets/a6c57191-0df9-4691-8792-0c329c6c3a0e)

Ahora revisamos que tenga la misma estructura física en localhost:4040 en la pestaña SQL / DataFrame

![image](https://github.com/user-attachments/assets/403a23a7-5013-4d19-a91a-d452fcfd1137)
![image](https://github.com/user-attachments/assets/dd4b1e26-4527-4c05-8de7-7b386a9f4277)
![image](https://github.com/user-attachments/assets/fb2bf927-10bf-4928-b723-b0bd80769ed1)

>>> result_df.show()

![image](https://github.com/user-attachments/assets/199c9803-6f38-4463-895c-5ff66cb73436)

3 — DATA INGESTION WITH PYSPARK
3.1.	Creación de archivo CSV con Spark
Corre el siguiente código en PySpark Shell.

>>> data = [(1, 'Mya', '2026-05-05'), (2, 'Allan', '2026-05-03'), (3, 'Kevin', '2026-05-01')]
>>> df_source = spark.createDataFrame(data, ['id', 'name', 'signup_date'])
>>> df_source.write.mode("overwrite").option("header", "true").csv("/opt/spark-                                                                                                                                          
        data/users_csv")

•	Directorio
![image](https://github.com/user-attachments/assets/493c5587-353c-4842-b5ae-c0939f3a75c2)

•	Spark UI (Jobs)
![image](https://github.com/user-attachments/assets/bbe57ad1-0aaa-4b72-a5ca-14197da71af9)

![image](https://github.com/user-attachments/assets/177cc9f4-24ff-4283-98cd-080a7c02b871)

•	PySpark UI (http://localhost:4040/SQL/execution/?id=5)

Muestra el ID de etapa y el ID de tarea que corresponden a la métrica máxima

![image](https://github.com/user-attachments/assets/b5753357-f4a5-489c-ba8e-adaff6b569a1)

>>> df = spark.read.format('csv').option('header', 'true').option('infraSchema', 
        'true').load('/opt/spark-data/users_csv')
>>> df.show()

![image](https://github.com/user-attachments/assets/dab5b924-b439-4a9b-8499-32c93f888d64)

* Spark UI

![image](https://github.com/user-attachments/assets/42b045a6-181b-4293-9ed1-2c49e1b807b8)

