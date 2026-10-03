from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Iniciar Spark
spark = SparkSession.builder \
    .appName("Kappa_V1_Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Definir el esquema basado en el productor
esquema = StructType() \
    .add("timestamp", TimestampType()) \
    .add("radar_id", StringType()) \
    .add("radar_azimut_deg", DoubleType()) \
    .add("co2_ppm", DoubleType()) \
    .add("velocidad_del_viento_ms", DoubleType()) \
    .add("direccion_del_viento", StringType())

# Leer desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "eventos") \
    .option("startingOffsets", "latest") \
    .load()

# Procesar JSON y calcular promedio de CO2 por ventana de 10s
df_parsed = df_kafka.select(from_json(col("value").cast("string"), esquema).alias("data")).select("data.*")

df_agrupado = df_parsed \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "10 seconds"), col("radar_id")) \
    .avg("co2_ppm", "velocidad_del_viento_ms")

# Escribir resultados en consola para la demostración
query = df_agrupado.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()
