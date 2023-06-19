from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import year
from pyspark.sql.functions import count

name = "Analisis 1"
spark = SparkSession.builder \
    .appName(name) \
    .config('spark.driver.extraClassPath', 'C://sqljdbc_12.2//enu//mssql-jdbc-12.2.0.jre11.jar') \
    .getOrCreate()

# Configura los detalles de conexión
server = "LAPTOP-8KE4ISMN"
DatabaseName = "BD_PruebaCaso5"
username = "mainUser"
password  = "1234"

tableUsuarios = "dbo.DS_Usuarios"
tableServicios = "Clean.serviciosCoseviClean"
tableCitas = "dbo.FT_Citas"

# Leer los datos en un DataFrame de PySpark
df_usuarios = spark.read.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable=tableUsuarios
    ).load()

df_servicios = spark.read.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable=tableServicios
    ).load()

df_citas = spark.read.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable=tableCitas
    ).load()

# Filtra las citas completadas (EstadoSolicitud = "Completo")
citas_completas_df = df_citas.filter(col("EstadoSolicitud") == "Completo")

# Realiza el conteo de citas completas por año
conteo_citas_anuales = citas_completas_df.groupBy(year(col("FechaSolicitud"))).agg(count(col("EstadoSolicitud")).alias("conteo_citas"))

# Realiza el cálculo de las ganancias anuales
ganancias_anuales_df = df_servicios.join(conteo_citas_anuales, df_servicios["Anio"] == conteo_citas_anuales["year(FechaSolicitud)"]) \
    .select(df_servicios["Servicio"], df_servicios["Anio"], conteo_citas_anuales["conteo_citas"], (df_servicios["Presupuesto"] + (df_servicios["Precio"] * conteo_citas_anuales["conteo_citas"]) - df_servicios["CostoAnual"]).alias("Ganancias")) \
    .groupBy("Servicio", "Anio", "conteo_citas", "Ganancias") \
    .count() \
    .orderBy("Anio")

ganancias_anuales_df.show()

# Escribe los datos en la tabla de SQL Server
ganancias_anuales_df.write.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable="GananciasServicio"
).mode("overwrite").save()

# Cierra la sesión de Spark
spark.stop()

