import os
import sys


#Configuración de entorno

ruta_java = r"C:\Java"
ruta_hadoop = r"D:\hadoop"

os.environ["JAVA_HOME"] = ruta_java
os.environ["HADOOP_HOME"] = ruta_hadoop

if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

#Reforzamos  el PATH
os.environ["COMSPEC"] = r"C:\Windows\System32\cmd.exe"
os.environ["PATH"] = ruta_java + r"\bin;" + ruta_hadoop + r"\bin;C:\Windows\System32;" + os.environ.get("PATH", "")

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

#Importación de pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

#Lógica principal del streaming
def main():
    print("Iniciando sesión de Spark...")
    
    spark = SparkSession.builder \
        .appName("Procesamiento_Kappa_V1") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("¡Spark iniciado con éxito! Esperando datos de Kafka...")

    esquema_co2 = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("radar_id", StringType(), True),
        StructField("radar_azimuth_deg", DoubleType(), True),
        StructField("co2_ppm", DoubleType(), True),
        StructField("wind_speed_ms", DoubleType(), True),
        StructField("wind_direction", StringType(), True)
    ])

    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "eventos") \
        .option("startingOffsets", "latest") \
        .load()

    df_procesado = df_kafka.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), esquema_co2).alias("data")) \
        .select("data.*")

    df_agrupado = df_procesado \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("radar_id")
        ) \
        .agg(avg("co2_ppm").alias("co2_promedio"))

    query = df_agrupado.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()