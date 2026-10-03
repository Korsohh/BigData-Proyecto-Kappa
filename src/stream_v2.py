from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, max
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Iniciar Spark
spark = SparkSession.builder \
    .appName("Kappa_V2_Reprocesamiento") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Definir el esquema
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
    .option("startingOffsets", "earliest") \
    .load()

# NUEVA LÓGICA: Ventana de 30 segundos y sacar el Viento Maximo
df_parsed = df_kafka.select(from_json(col("value").cast("string"), esquema).alias("data")).select("data.*")

df_agrupado = df_parsed \
    .withWatermark("timestamp", "30 seconds") \
    .groupBy(window(col("timestamp"), "30 seconds"), col("radar_id")) \
    .agg(max("velocidad_del_viento_ms").alias("viento_maximo_ms"))

# Escribir resultados
query = df_agrupado.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()
