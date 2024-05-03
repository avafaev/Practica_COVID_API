from pyspark.sql.functions import lit, to_date, col, monotonically_increasing_id, explode, array, rand, floor, date_add, split
from pyspark.sql.types import IntegerType, LongType, DoubleType
import random
import datetime
import requests
import json
import os


# --------------------------------------------------------------------------------#
# Descargar y guardar json desde la pagina web
def descargar_y_guardar_json(url, ruta_archivo):
    response = requests.get(url)
    if response.status_code == 200:
        datos_json = response.json()
        
        with open(ruta_archivo, 'w') as archivo:
            json.dump(datos_json, archivo)
        print()
        print("# Datos guardados exitosamente en", ruta_archivo)
        print()
    else:
        print("Error al descargar los datos. Código de estado:", response.status_code)


# --------------------------------------------------------------------------------#
# Generar json con ciudades y paises
def generar_pais_ciudad():
    # Datos de ciudades
    ciudades = [
        {"id": 1, "id_ciudad": 101, "Ciudad": "Madrid", "Coordenadas": (40.4168, -3.7038)},
        {"id": 2, "id_ciudad": 102, "Ciudad": "Paris", "Coordenadas": (41.3851, 2.1734)},
        {"id": 3, "id_ciudad": 103, "Ciudad": "Berlin", "Coordenadas": (39.4699, -0.3763)},
        {"id": 4, "id_ciudad": 104, "Ciudad": "Rome", "Coordenadas": (48.8566, 2.3522)},
        {"id": 5, "id_ciudad": 105, "Ciudad": "London", "Coordenadas": (52.5200, 13.4050)},
        {"id": 6, "id_ciudad": 106, "Ciudad": "Lisboa", "Coordenadas": (41.9028, 12.4964)}
    ]

    # Datos de países
    paises = [
        {"id": 1, "id_pais": 201, "Pais": "España"},
        {"id": 2, "id_pais": 202, "Pais": "Francia"},
        {"id": 3, "id_pais": 203, "Pais": "Alemania"},
        {"id": 4, "id_pais": 204, "Pais": "Italia"},
        {"id": 5, "id_pais": 205, "Pais": "Reino Unido"},
        {"id": 6, "id_pais": 206, "Pais": "Portugal"}
    ]

    # Obtener la ruta del directorio actual
    directorio_actual = os.path.dirname(os.path.realpath(__file__))

    # Crear la ruta de la carpeta g_datos en la misma ruta del script
    carpeta_g_datos = os.path.join(directorio_actual, "datos_generados")

    # Verificar si la carpeta existe, si no, crearla
    if not os.path.exists(carpeta_g_datos):
        os.makedirs(carpeta_g_datos)

    # Guardar los datos de ciudades en un archivo JSON dentro de la carpeta g_datos
    ruta_ciudades = os.path.join(carpeta_g_datos, "ciudades.json")
    with open(ruta_ciudades, "w") as archivo_ciudades:
        json.dump(ciudades, archivo_ciudades, indent=4)

    # Guardar los datos de países en un archivo JSON dentro de la carpeta g_datos
    ruta_paises = os.path.join(carpeta_g_datos, "paises.json")
    with open(ruta_paises, "w") as archivo_paises:
        json.dump(paises, archivo_paises, indent=4)

    print()
    print("# Datos de ciudades y países guardados exitosamente en la carpeta 'datos_generados'")
    print()


# --------------------------------------------------------------------------------#
# Funcion para generar 5 filas nuevas
def expand_df(df_entrada):
    df_with_id = df_entrada.withColumn("temp_id", monotonically_increasing_id())
    ids = array([(lit(i)) for i in range(1, 7)])
    tabla_duplicada = df_with_id.withColumn("id", explode(ids))
    
    columnas = ["id"] + [c for c in df_with_id.columns if c != "temp_id"]
    tabla_duplicada = tabla_duplicada.select(columnas)

    # Multiplicar columnas numéricas por un factor aleatorio entre 1 y 2.1, y redondear sin decimales
    for col_name in df_entrada.columns:
        # Verificar si el tipo de la columna es numérico y no es la columna 'date'
        if isinstance(df_entrada.schema[col_name].dataType, (DoubleType, IntegerType, LongType)) and col_name != "date":
            # Multiplicar, redondear hacia abajo y convertir a entero
            tabla_duplicada = tabla_duplicada.withColumn(col_name, floor(col(col_name) * (0.6 + rand() * 1.1)))
        
    # Transformar columna de date a Fecha de regalo
    tabla_duplicada = tabla_duplicada.withColumn('date', to_date(col('date').cast("string"), 'yyyyMMdd'))

    return tabla_duplicada


# --------------------------------------------------------------------------------#
# Funcion para multiplicar los datos por x veces
def expand_and_transform_json(spark, df_entrada, num_copies):
    # Crear un DataFrame con múltiplos y cambiar el tipo de 'copy_id'
    # Aquí, ajustamos para que 'multiplier' varíe de 0.7 a 1.5
    step_size = (1.5 - 0.7) / (num_copies - 1) if num_copies > 1 else 0
    multiplier_df = spark.range(num_copies).withColumn("multiplier", lit(0.7) + col("id") * lit(step_size))
    multiplier_df = multiplier_df.withColumn("copy_id", col("id").cast("integer")).drop("id")
    
    # Realizar un cross join con el DataFrame original
    expanded_df = df_entrada.crossJoin(multiplier_df)

    # Aplicar transformaciones a columnas numéricas
    for column in df_entrada.columns:
        if column not in ['date', 'id', 'id_pais', 'id_ciudad'] and isinstance(df_entrada.schema[column].dataType, (DoubleType, IntegerType, LongType)):
            # En lugar de una variación aleatoria, usar un rango específico 0.7 a 1.5
            expanded_df = expanded_df.withColumn(column, (col(column) * col("multiplier")).cast("integer"))
    
    # Ajustar la fecha según el número de copias
    if 'date' in df_entrada.columns:
        expanded_df = expanded_df.withColumn('date', date_add(col('date'), col('copy_id')))

    expanded_df = expanded_df.drop('multiplier', 'copy_id')
    
    return expanded_df


# --------------------------------------------------------------------------------#
# Eliminar columnas irrelevantes
def eliminar_columnas(df):
    # Lista de columnas a eliminar
    columnas_a_eliminar = ['id_pais', 'id_ciudad', 'dateChecked', 'hash', 'lastModified', 'posNeg', 'recovered', 'total']
    # Filtrar solo las columnas que existen en el DataFrame para evitar errores
    columnas_a_eliminar = [col for col in columnas_a_eliminar if col in df.columns]
    # Eliminar las columnas
    df_resultante = df.drop(*columnas_a_eliminar)
    return df_resultante


# --------------------------------------------------------------------------------#
# Transformar columna de 'date' e añadir fecha procesado
def transformar_datos(df):
    # Divide la columna 'date' en tres partes: año, mes y día
    df = df.withColumn("YYYY", split(col("date"), "-")[0]) \
           .withColumn("MM", split(col("date"), "-")[1]) \
           .withColumn("DD", split(col("date"), "-")[2])
    # Elimina la columna 'date' original
    df = df.drop("date")
    # Añade la columna 'fecha_procesado' con la fecha actual
    fecha_actual = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    df = df.withColumn("F_procesado", lit(fecha_actual))
    return df