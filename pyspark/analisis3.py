from pyspark.sql import SparkSession
from pyspark.sql.functions import col

name = "Analisis 3"
spark = SparkSession.builder \
    .appName(name) \
    .config('spark.driver.extraClassPath', 'C://sqljdbc_12.2//enu//mssql-jdbc-12.2.0.jre11.jar') \
    .getOrCreate()


server = "LAPTOP-8KE4ISMN"
DatabaseName = "BD_PruebaCaso5"
username = "mainUser"
password  = "1234"

tableUsuarios = "dbo.DS_Usuarios"
tablePaquetes = "dbo.DS_Paquetes"
tableCoseviServicios = "Clean.serviciosCoseviClean"
tableCitas = "dbo.FT_Citas"


df_citas = spark.read.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable=tableCitas
    ).load()

df_CleanCoserviServicios = spark.read.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable=tableCoseviServicios
    ).load()


citas_2020_2021_df = df_citas.filter((col("FechaSolicitud") >= "2020-01-01") & (col("FechaSolicitud") <= "2021-12-31"))
citas_sample_df = citas_2020_2021_df.sample(withReplacement=False, fraction=0.5, seed=42)

joined_df = df_CleanCoserviServicios.join(citas_sample_df, df_CleanCoserviServicios["id"] == citas_sample_df["IdServicio"], "inner")

result_df = joined_df.groupBy("Presupuesto", "EstadoSolicitud").count().orderBy("Presupuesto")

result_df.show()

result_df.write.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable="Prediccion_CompletitudCitas"
).mode("overwrite").save()

# Cierra la sesión de Spark
spark.stop()