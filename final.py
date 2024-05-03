from funciones import descargar_y_guardar_json, generar_pais_ciudad, expand_df,  expand_and_transform_json
from pyspark.sql import SparkSession
import pandas as pd
import warnings
import pyarrow

warnings.filterwarnings('ignore')


# --------------------------------------------------------------------------------#
# Inicializar sesion SPARK
spark = SparkSession.builder \
    .appName("Transformación de datos COVID 2") \
    .getOrCreate()


# --------------------------------------------------------------------------------#
# Descargar json con datos desde covidtracking
fecha = 20200506
url = f"https://api.covidtracking.com/v1/us/{fecha}.json"
ruta_archivo = "./downloads/datos_covid.json"
descargar_y_guardar_json(url, ruta_archivo)


# --------------------------------------------------------------------------------#
# Inicializar el json descargado en spark
json_entrada = spark.read.json('./downloads/datos_covid.json')


# --------------------------------------------------------------------------------#
# Añadir filas a json guardado para que tenga un total de 6 filas
json_mult = expand_df(json_entrada)


# --------------------------------------------------------------------------------#
# Generar y guardar 2 archivos json con ciudades y paises
generar_pais_ciudad()


# --------------------------------------------------------------------------------#
# Cargar el archivo JSON utilizando Pandas
pandas_ciudades = pd.read_json("./datos_generados/ciudades.json")
pandas_paises = pd.read_json("./datos_generados/paises.json")


# --------------------------------------------------------------------------------#
# Convertir el DataFrame de Pandas a un DataFrame de Spark
spark_ciudades = spark.createDataFrame(pandas_ciudades)
spark_paises= spark.createDataFrame(pandas_paises)


# --------------------------------------------------------------------------------#
# Juntar los dos DataFrames por la columna 'ID'
paises_ciudades_tabla = spark_paises.join(spark_ciudades, spark_paises['id'] == spark_ciudades['id']).select(
    spark_paises.id,
    spark_paises.id_pais,
    spark_ciudades.id_ciudad,
    spark_paises.Pais,
    spark_ciudades.Ciudad,
    spark_ciudades.Coordenadas
)


# --------------------------------------------------------------------------------#
# Unir por 'id' json de entrada con json de paises_ciudades
tabla_casi_final = paises_ciudades_tabla.join(json_mult, on="id", how="right")


# --------------------------------------------------------------------------------#
# Generar datos para 120 dias, lo que equivaldria a 4 meses
tabla_final = expand_and_transform_json(spark, tabla_casi_final, 120)


# --------------------------------------------------------------------------------#
# Transformar DataFrame de PySpark a DataFrame de Pandas
tabla_final_panda = tabla_final.toPandas()


# --------------------------------------------------------------------------------#
# Exportar archivo .parquet a la carpeta de 'datos_generados'
tabla_final_panda.to_parquet("datos_generados/tabla_datos_mult.parquet")
print('#------------------------------------------------------------------------------------------------#')
print()
print("     Documento creado y exportado satisfactoriamente dentro de la carpeta 'datos_generados'!")
print()
print('#------------------------------------------------------------------------------------------------#')


# --------------------------------------------------------------------------------#
# Detener sesion SPARK
spark.stop()