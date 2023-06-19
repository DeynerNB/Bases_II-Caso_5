from pyspark.sql import SparkSession
import pymssql

name = "PySpark SQL Server Example - via pymssql"
master = "local"
spark = SparkSession.builder \
    .appName(name) \
    .getOrCreate()

# Configura los detalles de conexión
server = "LAPTOP-8KE4ISMN"
database = "BD_PruebaCaso5"
username = "mainUser"
password  = "1234"
table = "dbo.DS_Usuarios"


# Establece la conexión
conn = pymssql.connect(server=server, database=database, user=username, password=password)

# Ejecuta una consulta
query = "SELECT * FROM DS_Usuarios"
cursor = conn.cursor()
cursor.execute(query)

# Obtiene los resultados de la consulta
results = cursor.fetchall()

# Crea un RDD a partir de los resultados
rdd = spark.sparkContext.parallelize(results)

# Crea el DataFrame
df = spark.createDataFrame(rdd)

# Muestra los datos
df.show()

# Cierra la conexión a SQL Server
conn.close()
# Cierra la sesión de Spark
spark.stop()
