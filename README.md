Laboratorio 03 Big Data: Arquitectura Kappa

Idea en una línea

Todos los datos se procesan como un flujo de eventos (streaming). Los eventos quedan guardados en Kafka, y si cambiamos la lógica, volvemos a leerlos desde el inicio (reprocesamiento).

Flujo

Productor (Python) → Kafka (topic) → Spark Streaming (ventanas) → CSV/SQLite

Pasos

1. Preparar el entorno
- Instalar y abrir Docker Desktop para Windows.
- Instalar Java (JDK), Python y Spark en Windows (Spark necesita winutils y HADOOP_HOME).
- En una terminal, desde la carpeta del proyecto, iniciar Kafka con `docker compose up -d`.
- Verificar el contenedor con `docker compose ps`; consultar sus registros con `docker compose logs -f kafka`.
- Crear el topic de eventos una sola vez:
	`docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic eventos --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --config retention.ms=604800000`
- Confirmar que existe con:
	`docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092`
- Instalar las dependencias de Python cuando exista el archivo de requisitos: `pip install -r requirements.txt`.

Kafka queda disponible para los programas Python ejecutados en Windows en `localhost:9092`. Los datos del broker se conservan en el volumen Docker `kafka_data`, incluso después de detener los contenedores con `docker compose down`; no elimines ese volumen si necesitas reprocesar el historial.

Para detener Kafka: `docker compose down`.

3. Definir los datos

-Elegir qué eventos simulamos.

-Definir los campos (ej: usuario, valor, etc).

4. Crear el productor

- Configuración previa en Windows:
  1. Seleccionar Python 3.12 en VS Code: presionar `Ctrl + Shift + P`, escribir y seleccionar `Python: Select Interpreter`, y elegir Python 3.12.
  2. Limpiar versiones previas con el comando: `py -3.12 -m pip uninstall kafka kafka-python kafka-python-ng -y`
  3. Instalar librería Kafka de la forma corta `py -3.12 -m pip install kafka-python-ng`
  4. Iniciar la depuración para ver los datos generados.

- src/producer.py: envía un evento cada segundo al topic de Kafka.

- Comprobar que los mensajes llegan al topic.

5. Procesar en streaming (v1)

-src/stream_v1.py: lee el topic con Spark Structured Streaming.

-Agrupar por ventanas de 10 segundos (ej: suma de eventos por usuario).

-Guardar los resultados en output/v1/ (CSV o SQLite).

6. Reprocesar (v2)

-Cambiar la lógica (ej: otra ventana o otra métrica) en src/stream_v2.py.

-Volver a leer el historial desde el inicio del topic (startingOffsets = earliest).

-Guardar resultados en output/v2/ y comparar con v1.

7. Documentar

-Sacar pantallazos de cada paso en docs/evidencias/.

-Redactar el proceso en docs/informe.md.
-Anotar los errores y soluciones.

8. Entregar
-Tabla comparativa Lambda vs Kappa vs Data Lakehouse.
-Código en .zip
-Presentación de 12 a 15 minutos
