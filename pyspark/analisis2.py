from pyspark.sql import SparkSession
from pyspark.sql.functions import col

name = "Analisis 2"
spark = SparkSession.builder \
    .appName(name) \
    .config('spark.driver.extraClassPath', 'C://sqljdbc_12.2//enu//mssql-jdbc-12.2.0.jre11.jar') \
    .getOrCreate()

# Configura los detalles de conexión
server = "LAPTOP-8KE4ISMN"
DatabaseName = "BD_PruebaCaso5"
username = "mainUser"
password  = "1234"

tablePaquetes = "dbo.DS_Paquetes"
tableCoseviServicios = "Clean.serviciosCoseviClean"

# Cargar la información de las tablas
df_paquetes = spark.read.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable=tablePaquetes
    ).load()
df_CleanCoserviServicios = spark.read.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable=tableCoseviServicios
    ).load()

# Calcular el valor de CostoExtra
costo_extra = df_paquetes.filter(col("nombre") == " vip") \
    .select(col("precio_del_paquete")) \
    .first()[0] * 12 * 564

# Realizar el cálculo del NuevoCosto
df_CostoOperativo = df_CleanCoserviServicios.withColumn("NuevoCosto", col("CostoAnual") / 3 + costo_extra) \
    .select("Servicio", "Anio", "CostoAnual", (col("NuevoCosto").cast("int")).alias("NuevoCosto")) \
    .groupBy("Servicio", "Anio", "CostoAnual", "NuevoCosto") \
    .count() \
    .orderBy("Anio")

# Mostrar el resultado (Solo consola)
df_CostoOperativo.show()

# Escribe los datos en la tabla de SQL Server -> dbo.Analisis_CostoOperativo
df_CostoOperativo.write.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable="Analisis_CostoOperativo"
).mode("overwrite").save()

# Cierra la sesión de Spark
spark.stop()

