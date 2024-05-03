from funciones import eliminar_columnas, transformar_datos
from pyspark.sql import SparkSession
import pandas as pd
import shutil
import os


# --------------------------------------------------------------------------------#
# Configurar la sesión de Spark
spark = SparkSession.builder \
    .appName("Transformación datos parquet") \
    .getOrCreate()


# --------------------------------------------------------------------------------#
# Leer archivo Parquet
df = spark.read.parquet("./datos_generados/tabla_datos_mult.parquet")


# --------------------------------------------------------------------------------#
# Aplicar funcion para eliminar columnas irrelevantes
df_nuevo = eliminar_columnas(df)


# --------------------------------------------------------------------------------#
# Transformar columna de 'date' e añadir fecha procesado
df_n = transformar_datos(df_nuevo)


# --------------------------------------------------------------------------------#
# Transformar DataFrame de PySpark a DataFrame de Pandas
tabla_final_panda = df_n.toPandas()


# --------------------------------------------------------------------------------#
# Exportar archivo .parquet a la carpeta de 'datos_generados'
tabla_final_panda.to_parquet("./datos_generados/tabla_procesada.parquet")


# --------------------------------------------------------------------------------#
# Mover archivos viejos/procesados
# # Lista de nombres de archivos para mover
nombres_archivos = ['ciudades.json', 'paises.json', 'tabla_datos_mult.parquet']
# # Directorio original y destino
directorio_original = 'datos_generados'
directorio_destino = 'old'
# # Asegurándonos de que la carpeta destino existe
os.makedirs(directorio_destino, exist_ok=True)
# # Proceso de mover cada archivo
for nombre_archivo in nombres_archivos:
    ruta_original = os.path.join(directorio_original, nombre_archivo)
    ruta_destino = os.path.join(directorio_destino, nombre_archivo)

    try:
        shutil.move(ruta_original, ruta_destino)
        print()
        print(f"# Archivo {nombre_archivo} movido exitosamente a {directorio_destino}!")
        print()
    except Exception as e:
        print(f"Ocurrió un error al mover el archivo {nombre_archivo}: {e} ;(")

print('#------------------------------------------------------------------------------------------------#')
print()
print("  Documento transformado y exportado satisfactoriamente dentro de la carpeta 'datos_generados'!")
print()
print('#------------------------------------------------------------------------------------------------#')
# --------------------------------------------------------------------------------#
# Detener sesion SPARK
spark.stop()