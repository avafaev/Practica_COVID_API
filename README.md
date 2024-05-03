**Instrucciones para ejecutar y poder visualizar los resultados de una forma correcta:**

1.    Ejecutar el documento python: 'finaly.py'
        - Windows: python final.py
          
      - Al ejecutar este python, el programa realizara los siguientes pasos:
          - a) Descargar json con datos desde covidtracking
          - b) Inicializar el json descargado en spark
          - c) Añadir filas a json guardado para que tenga un total de 6 filas
          - d) Generar y guardar 2 archivos json con ciudades y paises
          - e) Cargar el archivo JSON utilizando Pandas
          - f) Convertir el DataFrame de Pandas a un DataFrame de Spark
          - g) Juntar los dos DataFrames por la columna 'ID'
          - h) Unir por 'id' json de entrada con json de paises_ciudades
          - i) Generar datos para 120 dias, lo que equivaldria a 4 meses
          - j) Transformar DataFrame de PySpark a DataFrame de Pandas
          - k) Exportar archivo .parquet a la carpeta de 'datos_generados'

2.    Ejecutar el documento python: 'procesado.py'
        - Windows: python procesado.py
          
      - Al ejecutar este python, el programa realizara los siguientes pasos:
          - a) Leer archivo Parquet generado previamente
          - b) Aplicar funcion para eliminar columnas irrelevantes
          - c) Transformar columna de 'date' e añadir fecha procesado
          - d) Transformar DataFrame de PySpark a DataFrame de Pandas
          - e) Exportar archivo .parquet a la carpeta de 'datos_generados'
          - f) Mover archivos viejos/procesados hacia la carpeta /old
          - g) Exportar el documento transformado dentro de la carpeta /datos_generados

3. Visualizar el resultado en PowerBI, este esta ubicado dentro de la carpeta /power_bi.
      
