# Informe Técnico: Implementación Arquitectura Kappa en Windows

## Introducción
El presente laboratorio documenta la implementación de una arquitectura Kappa simulando un entorno Big Data local. Se eligió esta arquitectura debido a su capacidad para tratar todos los datos como flujos continuos de eventos, evitando la complejidad de mantener dos capas de procesamiento separadas (como en Lambda) y permitiendo el reprocesamiento histórico desde un único registro inmutable.

## Análisis Comparativo de Arquitecturas
| Criterio | Lambda | Kappa | Data Lakehouse |
| :--- | :--- | :--- | :--- |
| **Procesamiento** | Capa batch y speed separadas. | Todo es flujo continuo (streaming). | Batch, SQL y distribuido. |
| **Complejidad** | Alta (mantiene dos lógicas). | Media (centraliza lógica). | Variable (depende del gobierno). |
| **Uso Ideal** | Análisis histórico + baja latencia. | Eventos en tiempo real, IoT, telemetría. | Plataformas de IA, BI y analítica. |

## Desarrollo y Evidencias
### Infraestructura (Kafka en Docker)
Se levantó Apache Kafka y Zookeeper utilizando contenedores Docker en Windows.
*Evidencia 1: Pantallazo de los contenedores corriendo (docker compose ps).*

### Ingesta de Datos (Productor Python)
Se simuló la emisión de telemetría de radares meteorológicos (CO2, viento) inyectando datos en formato JSON cada 1 segundo al tópico `eventos`.
*Evidencia 2: Pantallazo de la consola de Python mostrando el envío de logs.*

### Procesamiento Streaming (PySpark - V1)
Se utilizó Spark Structured Streaming para leer desde Kafka y agregar los datos en ventanas temporales de 10 segundos.
*Evidencia 3: Pantallazo de la salida en consola/CSV de la V1.*

### Demostración de Reprocesamiento (V2)
Se modificó la lógica de agregación (ventanas de 30s) y se leyó el historial completo de Kafka utilizando `startingOffsets: earliest`.
*Evidencia 4: Pantallazo del reprocesamiento exitoso.*

## Resolución de Problemas y Tips

