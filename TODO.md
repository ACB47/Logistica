# TODO.md

Checklist para cerrar el proyecto integral de Big Data segun el enunciado de `Proyecto Big Data.pdf` y el estado actual del repositorio.

## 0. Estado actual resumido
- [x] Hay stack Docker con Kafka, Spark, Cassandra, Hive/HDFS, Airflow, Zeppelin y NiFi.
- [x] Hay productores Kafka y un consumidor `kafka_to_hdfs_raw.py` para landing raw en HDFS.
- [x] Hay jobs Spark para staging, analitica de grafos y scoring de alertas.
- [x] Hay un DAG de Airflow funcional para micro-lotes.
- [x] Hay utilidades de verificacion y algo de documentacion base.
- [ ] El proyecto todavia no cumple completamente el enunciado 2025-2026 tal como esta pedido.

## 1. Alinear versiones y arquitectura con la rubrica
- [ ] Actualizar `docker-compose.yml` y `docker-compose.simple.yml` para acercarlos al stack exigido:
  - Kafka `3.9.1+` en modo KRaft.
  - NiFi `2.6.0+`.
  - Airflow `2.10.x` real en despliegue principal.
  - Cassandra `5.0+` si es viable en el entorno.
  - HDFS `3.4.2+` y uso claro de YARN.
- [ ] Eliminar la dependencia de ZooKeeper en la ruta principal de ejecucion si la entrega final va a defender KRaft.
- [ ] Decidir y documentar una arquitectura final unica: Docker demo, cluster 3 VMs, o ambas.
- [ ] Validar que el camino principal de ejecucion coincide con lo que se mostrara en la defensa.

## 2. Fase I - Ingesta y seleccion
### Fuentes y NiFi
- [ ] Implementar un flujo real en NiFi para consumir al menos una API publica exigida por el enunciado.
- [ ] Elegir la fuente externa final:
  - OpenWeather.
  - otra API publica estable y documentable.
- [ ] Modelar el flujo NiFi con processors equivalentes a:
  - `InvokeHTTP`.
  - transformacion/normalizacion JSON.
  - `RouteOnAttribute`.
  - `PublishKafka...` y/o `PutHDFS`.
- [ ] Configurar back-pressure, reintentos y cola de errores en NiFi.
- [ ] Guardar evidencia exportable del flujo NiFi para la memoria y la demo.

### Kafka
- [x] Existe topic `datos_crudos`.
- [x] Existe topic `alertas_globales`.
- [ ] Crear y usar un topic de `datos_filtrados` o equivalente, porque el enunciado pide separar datos crudos y filtrados.
- [ ] Definir claramente el contrato de eventos por topic:
  - posiciones GPS.
  - alertas globales.
  - eventos de stock o datos maestros si se incorporan.
- [ ] Añadir scripts/comandos de verificacion de topics, particiones y retencion.
- [ ] Probar flujo extremo a extremo NiFi -> Kafka -> consumidor.

### Raw landing y auditoria
- [x] Ya existe landing raw en HDFS desde Kafka.
- [ ] Confirmar que las rutas raw finales coinciden con las que se van a enseñar en la entrega.
- [ ] Incorporar trazabilidad minima: timestamps, origen, tipo de evento, ruta de error si falla la ingesta.
- [ ] Generar capturas o logs de auditoria para demostrar persistencia raw.

## 3. Fase II - Preprocesamiento y transformacion con Spark
### Limpieza y normalizacion
- [x] `jobs/spark/01_raw_to_staging.py` limpia, tipa y deduplica datos.
- [ ] Revisar que todos los timestamps queden normalizados de forma consistente para batch y streaming.
- [ ] Revisar naming y esquema final de staging para que coincidan con la memoria y la defensa.
- [ ] Añadir una validacion automatica minima del schema de staging y conteos por tabla.

### Enriquecimiento con datos maestros en Hive
- [ ] Crear tablas maestras reales en Hive:
  - `dim_ports`.
  - `dim_routes`.
  - `dim_warehouse`.
  - `dim_skus` o equivalente.
- [ ] Enriquecer el pipeline Spark cruzando raw/staging con esas dimensiones Hive, no solo con valores simulados embebidos.
- [ ] Documentar el origen de cada dimension y el criterio de join.
- [ ] Validar que las tablas Hive quedan consultables desde Spark y desde la UI/CLI elegida.

### Grafos con GraphFrames
- [x] Ya existe `jobs/spark/02_graph_metrics.py`.
- [ ] Confirmar si el analisis final va a centrarse en camino mas corto, criticidad o comunidades; idealmente cubrir al menos dos metricas.
- [ ] Mejorar el modelo del grafo para acercarlo al enunciado:
  - nodos = almacenes y puertos.
  - aristas = rutas con peso por tiempo y/o riesgo.
- [ ] Incorporar una salida mas defendible para nodos criticos o rutas congestionadas.
- [ ] Generar tablas/resultados listos para captura en Hive o Zeppelin.

## 4. Fase III - Mineria y accion
### Structured Streaming
- [x] Existe `jobs/spark/04_streaming_ml_pipeline.py` con `readStream` y `writeStream`.
- [ ] Ajustar las ventanas a `15 minutes`, porque el enunciado lo pide explicitamente y ahora el codigo usa `5 minutes`.
- [ ] Verificar que el pipeline streaming realmente corre en el entorno final elegido.
- [ ] Definir si la entrega final usa:
  - streaming real con Structured Streaming, o
  - micro-batch documentado como aproximacion.
- [ ] Si se defiende streaming real, integrar ese job en la operativa principal y no dejarlo solo como codigo aislado.

### Persistencia multicapa
- [x] Hive se usa para staging y facts historicos.
- [x] Cassandra tiene setup y tablas creadas.
- [ ] Implementar el caso de uso exigido para Cassandra: ultimo estado conocido de cada vehiculo para consultas de baja latencia.
- [ ] Revisar el modelo Cassandra actual para que responda a preguntas reales de negocio y no solo a metricas agregadas.
- [ ] Validar escrituras reales en Cassandra desde Spark o desde un proceso dedicado.
- [ ] Preparar consultas de demostracion en Cassandra para la defensa.

### ML, reglas y accion operativa
- [x] Hay scoring y recomendaciones en `jobs/spark/03_score_and_alert.py`.
- [x] Hay un pipeline ML experimental en `jobs/spark/04_streaming_ml_pipeline.py`.
- [ ] Decidir el enfoque final que se va a defender:
  - reglas de negocio + grafo.
  - ML + grafo.
  - enfoque hibrido.
- [ ] Asegurar que el modelo o scoring final tenga entradas, salida y criterio de evaluacion claros.
- [ ] Integrar el envio de alertas email con configuracion segura por `.env` o variables de entorno.
- [ ] Probar y evidenciar al menos una alerta real generada de punta a punta.

## 5. Fase IV - Orquestacion con Airflow
- [x] Existe `airflow/dags/logistica_kdd_dag.py`.
- [x] El DAG ya tiene dependencias y reintentos basicos.
- [ ] Adaptar el DAG al requisito del enunciado: reentrenamiento mensual del modelo de grafos y limpieza de tablas temporales HDFS.
- [ ] Separar tareas del DAG para que reflejen mejor el ciclo KDD:
  - ingest/landing.
  - staging.
  - dimensiones.
  - grafo.
  - scoring.
  - limpieza.
- [ ] Añadir manejo de fallo mas visible: email, log estructurado o callback de error.
- [ ] Verificar el DAG en la version final de Airflow que se vaya a presentar.
- [ ] Preparar evidencias: vista Graph, run exitoso y al menos un reintento.

## 6. YARN y ejecucion distribuida
- [ ] Ejecutar al menos un job Spark real sobre YARN, porque el enunciado lo pide explicitamente.
- [ ] Guardar comando exacto y evidencia de ejecucion en cluster.
- [ ] Verificar ResourceManager UI y consumo de recursos.
- [ ] Alinear scripts y documentacion para que la ruta YARN sea reproducible.

## 7. Calidad, pruebas y validacion final
- [ ] Crear un bloque minimo de validaciones reproducibles:
  - `docker-compose config`.
  - `python3 -m compileall ingesta jobs airflow`.
  - smoke test de productores y consumidor.
  - al menos un `spark-submit` clave.
  - prueba del DAG.
- [ ] Añadir tests automatizados si da tiempo, al menos para helpers puros de Python.
- [ ] Crear un guion de demo de extremo a extremo de 5-10 minutos.
- [ ] Verificar que no quedan credenciales reales hardcodeadas.
- [ ] Sustituir placeholders SMTP y revisar `.env.example`.

## 8. Documentacion y memoria
- [x] Ya existe `docs/00_documento_word.md` como base.
- [ ] Completar la memoria siguiendo el ciclo KDD completo.
- [ ] Incluir diagrama de arquitectura final.
- [ ] Incluir justificacion tecnica de cada tecnologia usada.
- [ ] Incluir capturas obligatorias:
  - HDFS.
  - YARN.
  - Kafka topics.
  - NiFi canvas.
  - Hive tablas.
  - GraphFrames resultados.
  - Airflow DAG.
  - Cassandra consultas.
- [ ] Explicar claramente que parte esta implementada de verdad y que parte es simulada.
- [ ] Redactar conclusiones, limitaciones y mejoras futuras.

## 9. Zeppelin y visualizacion
- [x] Existen notebooks Zeppelin en `zeppelin/`.
- [ ] Revisar que los notebooks abren correctamente y usan datos actuales.
- [ ] Preparar al menos un notebook de overview ejecutivo y otro tecnico de alertas/rutas.
- [ ] Capturar visualizaciones utiles para la memoria.

## 10. Cierre operativo del proyecto
- [ ] Definir una ruta oficial de ejecucion desde cero.
- [ ] Probar esa ruta en limpio en la maquina final o en las VMs finales.
- [ ] Resolver incoherencias entre Docker, scripts standalone y cluster de 3 VMs.
- [ ] Confirmar que todos los comandos del README funcionan o actualizarlos.
- [ ] Dejar una checklist de entrega final marcada con fecha.

## 11. Checklist de entrega final
- [ ] Stack final levantado sin errores.
- [ ] Ingesta real demostrable desde API/NI-Fi/Kafka.
- [ ] Copia raw en HDFS demostrable.
- [ ] Staging y curated en Hive demostrables.
- [ ] Grafo y metricas defendibles demostrables.
- [ ] Persistencia Cassandra demostrable para baja latencia.
- [ ] DAG Airflow ejecutado con exito.
- [ ] Al menos una alerta operativa generada y registrada.
- [ ] Memoria completa con capturas.
- [ ] Demo final ensayada.
