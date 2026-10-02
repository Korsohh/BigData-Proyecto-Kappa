Laboratorio 03 Big Data: Arquitectura Kappa

Idea en una línea
Todos los datos se procesan como un flujo de eventos (streaming). Los eventos quedan guardados en Kafka, y si cambiamos la lógica, volvemos a leerlos desde el inicio (reprocesamiento).

Flujo
Productor (Python) → Kafka (topic) → Spark Streaming (ventanas) → CSV/SQLite

Pasos

1. Preparar el entorno
-Instalar Java (JDK), Python y Spark en Windows (Spark necesita winutils y HADOOP_HOME).
-Levantar Kafka con Docker (docker-compose.yml).
-Instalar dependencias: pip install -r requirements.txt.

2. Definir los datos
-Elegir qué eventos simulamos.
-Definir los campos (ej: usuario, valor, etc).

3. Crear el productor
-src/producer.py: envía un evento cada segundo al topic de Kafka.
-Comprobar que los mensajes llegan al topic.

4. Procesar en streaming (v1)
-src/stream_v1.py: lee el topic con Spark Structured Streaming.
-Agrupar por ventanas de 10 segundos (ej: suma de eventos por usuario).
-Guardar los resultados en output/v1/ (CSV o SQLite).

5. Reprocesar (v2)
-Cambiar la lógica (ej: otra ventana o otra métrica) en src/stream_v2.py.
-Volver a leer el historial desde el inicio del topic (startingOffsets = earliest).
-Guardar resultados en output/v2/ y comparar con v1.

6. Documentar
-Sacar pantallazos de cada paso en docs/evidencias/.
-Redactar el proceso en docs/informe.md.
-Anotar los errores y soluciones.

7. Entregar
-Tabla comparativa Lambda vs Kappa vs Data Lakehouse.
-Código en .zip
-Presentación de 12 a 15 minutos
