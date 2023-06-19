from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import DenseVector
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.sql.functions import udf

name = "Analisis 4"
spark = SparkSession.builder \
    .appName(name) \
    .config('spark.driver.extraClassPath', 'C://sqljdbc_12.2//enu//mssql-jdbc-12.2.0.jre11.jar') \
    .getOrCreate()

# Configura los detalles de conexión
server = "LAPTOP-8KE4ISMN"
DatabaseName = "BD_PruebaCaso5"
username = "mainUser"
password  = "1234"

tableCostoOperativo = "dbo.Analisis_CostoOperativo"
tableGanancias = "dbo.GananciasServicio"

# Definir una función UDF para convertir la columna "features" a un tipo de dato compatible
convert_to_dense_vector = udf(lambda features: DenseVector(features.toArray()), ArrayType(DoubleType()))

# Cargar la información de las tablas
df_costoOperativo = spark.read.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable=tableCostoOperativo
    ).load()
df_Ganancias = spark.read.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable=tableGanancias
    ).load()

# Unir la tabla de citas con las dimensiones de Ganancias y Costom operativo
joined_data = df_Ganancias.join(df_costoOperativo, df_Ganancias["Servicio"] == df_costoOperativo["Servicio"]) \
    .select(df_Ganancias["Servicio"], df_Ganancias["Anio"], "CostoAnual", "Ganancias")

# Crear un ensamblador de características
assembler = VectorAssembler(inputCols=["CostoAnual", "Ganancias"], outputCol="features")

# Ensamblar las características en un solo vector
assembled_data = assembler.transform(joined_data)

# Dividir los datos en conjuntos de entrenamiento y prueba
train_data, test_data = assembled_data.randomSplit([0.7, 0.3])

# Crear un modelo de regresión lineal
lr = LinearRegression(labelCol="Ganancias")

# Entrenar el modelo utilizando los datos de entrenamiento
model = lr.fit(train_data)

# Realizar predicciones en los datos de prueba
predictions = model.transform(test_data).select("Servicio", "Anio", "Ganancias", "prediction")

# Mostrar las predicciones
predictions.show()

# Escribe los datos en la tabla de SQL Server -> dbo.Analisis_CostoOperativo
predictions.write.format("jdbc").options(
    url=f"jdbc:sqlserver://{server}:1433;DatabaseName={DatabaseName};user={username};password={password};encrypt=true;trustServerCertificate=true;",
    dbtable="Prediccion_PrevisionIngresos"
).mode("overwrite").save()

# Cierra la sesión de Spark
spark.stop()

