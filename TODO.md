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
- [x] Actualizar `docker-compose.yml` y `docker-compose.simple.yml` para acercarlos al stack exigido:
  - Kafka `3.9.1+` en modo KRaft.
  - NiFi `2.6.0+`.
  - Airflow `2.10.x` real en despliegue principal.
  - Cassandra `5.0+` si es viable en el entorno.
  - HDFS `3.4.2+` y uso claro de YARN.
- [x] Eliminar la dependencia de ZooKeeper en la ruta principal de ejecucion si la entrega final va a defender KRaft.
- [x] Decidir y documentar una arquitectura final unica: Docker demo, cluster 3 VMs, o ambas.
- [x] Validar que el camino principal de ejecucion coincide con lo que se mostrara en la defensa.

## 2. Fase I - Ingesta y seleccion
### Fuentes y NiFi
- [x] Implementar un flujo real en NiFi para consumir al menos una API publica exigida por el enunciado.
- [x] Elegir la fuente externa final:
  - OpenWeather.
  - otra API publica estable y documentable.
- [x] Modelar el flujo NiFi con processors equivalentes a:
  - `InvokeHTTP`.
  - transformacion/normalizacion JSON.
  - `RouteOnAttribute`.
  - `PublishKafka...` y/o `PutHDFS`.
- [x] Configurar back-pressure, reintentos y cola de errores en NiFi.
- [x] Guardar evidencia exportable del flujo NiFi para la memoria y la demo.

### Kafka
- [x] Existe topic `datos_crudos`.
- [x] Existe topic `alertas_globales`.
- [x] Crear y usar un topic de `datos_filtrados` o equivalente, porque el enunciado pide separar datos crudos y filtrados.
- [x] Definir claramente el contrato de eventos por topic:
  - posiciones GPS.
  - alertas globales.
  - eventos de stock o datos maestros si se incorporan.
- [x] Añadir scripts/comandos de verificacion de topics, particiones y retencion.
- [x] Probar flujo extremo a extremo NiFi -> Kafka -> consumidor.

### Raw landing y auditoria
- [x] Ya existe landing raw en HDFS desde Kafka.
- [x] Confirmar que las rutas raw finales coinciden con las que se van a enseñar en la entrega.
- [x] Incorporar trazabilidad minima: timestamps, origen, tipo de evento, ruta de error si falla la ingesta.
- [ ] Generar capturas o logs de auditoria para demostrar persistencia raw.

## 3. Fase II - Preprocesamiento y transformacion con Spark
### Limpieza y normalizacion
- [x] `jobs/spark/01_raw_to_staging.py` limpia, tipa y deduplica datos.
- [x] Revisar que todos los timestamps queden normalizados de forma consistente para batch y streaming.
- [x] Revisar naming y esquema final de staging para que coincidan con la memoria y la defensa.
- [x] Añadir una validacion automatica minima del schema de staging y conteos por tabla.

### Enriquecimiento con datos maestros en Hive
- [x] Crear tablas maestras reales en Hive:
  - `dim_ports`.
  - `dim_routes`.
  - `dim_warehouse`.
  - `dim_skus` o equivalente.
- [x] Enriquecer el pipeline Spark cruzando raw/staging con esas dimensiones Hive, no solo con valores simulados embebidos.
- [x] Documentar el origen de cada dimension y el criterio de join.
- [x] Validar que las tablas Hive quedan consultables desde Spark y desde la UI/CLI elegida.

### Grafos con GraphFrames
- [x] Ya existe `jobs/spark/02_graph_metrics.py`.
- [x] Confirmar si el analisis final va a centrarse en camino mas corto, criticidad o comunidades; idealmente cubrir al menos dos metricas.
- [x] Mejorar el modelo del grafo para acercarlo al enunciado:
  - nodos = almacenes y puertos.
  - aristas = rutas con peso por tiempo y/o riesgo.
- [x] Incorporar una salida mas defendible para nodos criticos o rutas congestionadas.
- [x] Generar tablas/resultados listos para captura en Hive o Zeppelin.

## 4. Fase III - Mineria y accion
### Structured Streaming
- [x] Existe `jobs/spark/04_streaming_ml_pipeline.py` con `readStream` y `writeStream`.
- [x] Ajustar las ventanas a `15 minutes`, porque el enunciado lo pide explicitamente y ahora el codigo usa `5 minutes`.
- [x] Verificar que el pipeline streaming realmente corre en el entorno final elegido.
- [x] Definir si la entrega final usa:
  - streaming real con Structured Streaming, o
  - micro-batch documentado como aproximacion.
- [x] Decidir defensa por `micro-batch documentado` para reducir riesgo operativo, manteniendo streaming real como evidencia tecnica complementaria.
- [x] El streaming queda como evidencia tecnica complementaria (no integrado en operativa principal).

### Persistencia multicapa
- [x] Hive se usa para staging y facts historicos.
- [x] Cassandra tiene setup y tablas creadas.
- [x] Implementar el caso de uso exigido para Cassandra: ultimo estado conocido de cada vehiculo para consultas de baja latencia.
- [x] Revisar el modelo Cassandra actual para que responda a preguntas reales de negocio y no solo a metricas agregadas.
- [x] Validar escrituras reales en Cassandra desde Spark o desde un proceso dedicado.
- [x] Preparar consultas de demostracion en Cassandra para la defensa.
- [x] Modelar una fact table de contingencia `barco vs aereo+camion` con ETA, coste y riesgo de stock.

### ML, reglas y accion operativa
- [x] Hay scoring y recomendaciones en `jobs/spark/03_score_and_alert.py`.
- [x] Hay un pipeline ML experimental en `jobs/spark/04_streaming_ml_pipeline.py`.
- [x] Decidir el enfoque final que se va a defender:
  - reglas de negocio + grafo.
  - ML + grafo.
  - enfoque hibrido.
- [x] Enfoque final elegido: reglas de negocio + grafo para la defensa; ML queda como evidencia experimental complementaria.
- [ ] Asegurar que el modelo o scoring final tenga entradas, salida y criterio de evaluacion claros.
- [x] Integrar el envio de alertas email con configuracion segura por `.env` o variables de entorno.
- [x] Probar y evidenciar al menos una alerta real generada de punta a punta.
- [x] Incorporar un caso de uso avanzado de contingencia con recomendacion explicable para acelerar envios sin romper stock.

## 5. Fase IV - Orquestacion con Airflow
- [x] Existe `airflow/dags/logistica_kdd_dag.py`.
- [x] El DAG ya tiene dependencias y reintentos basicos.
- [x] Adaptar el DAG al requisito del enunciado: reentrenamiento mensual del modelo de grafos y limpieza de tablas temporales HDFS.
- [x] Separar tareas del DAG para que reflejen mejor el ciclo KDD:
  - ingest/landing.
  - staging.
  - dimensiones.
  - grafo.
  - scoring.
  - limpieza.
- [x] Añadir manejo de fallo mas visible: email, log estructurado o callback de error.
- [x] Verificar el DAG en la version final de Airflow que se vaya a presentar.
- [x] Preparar evidencias: vista Graph, run exitoso y al menos un reintento.

## 6. YARN y ejecucion distribuida
- [ ] Documentar en la memoria que la entrega final no usa YARN/VMs y justificar la ruta Docker/local elegida.

## 7. Calidad, pruebas y validacion final
- [x] Crear un bloque minimo de validaciones reproducibles:
  - `docker-compose config`.
  - `python3 -m compileall ingesta jobs airflow`.
  - smoke test de productores y consumidor.
  - al menos un `spark-submit` clave.
  - prueba del DAG.
- [ ] Añadir tests automatizados si da tiempo, al menos para helpers puros de Python.
- [x] Crear un guion de demo de extremo a extremo de 5-10 minutos.
- [x] Verificar que no quedan credenciales reales hardcodeadas.
- [x] Sustituir placeholders SMTP y revisar `.env.example`.
- [x] Estabilizar el rebuild tecnico tras reinicios: metastore Hive persistente + script `scripts/66_rebuild_hive_demo_tables.sh` validado.

## 8. Documentacion y memoria
- [x] Ya existe `docs/00_documento_word.md` como base.
- [x] Completar la memoria siguiendo el ciclo KDD completo.
- [x] Incluir diagrama de arquitectura final.
- [x] Incluir justificacion tecnica de cada tecnologia usada.
- [x] Preparar checklist exacto de evidencias y comandos para la memoria/demo.
- [ ] Incluir capturas obligatorias:
  - [x] Kafka topics.
  - [x] NiFi canvas.
  - [x] Hive tablas base (`stg_weather_open_meteo`, `dim_ports_routes_weather`, `fact_weather_operational`).
  - [x] Hive alertas (`fact_alerts`).
  - [x] Hive contingencia (`fact_air_recovery_options`).
  - [x] Cassandra consultas.
  - [x] HDFS curated.
  - [x] GraphFrames resultados.
  - [x] Airflow DAG.
  - [x] Dashboard final con stock Valladolid, pedidos Douai y Gantt.
- [ ] Explicar claramente que parte esta implementada de verdad y que parte es simulada.
- [ ] Redactar conclusiones, limitaciones y mejoras futuras.

## 9. Zeppelin y visualizacion
- [x] Existen notebooks Zeppelin en `zeppelin/`.
- [ ] Revisar que los notebooks abren correctamente y usan datos actuales.
- [ ] Preparar al menos un notebook de overview ejecutivo y otro tecnico de alertas/rutas.
- [ ] Capturar visualizaciones utiles para la memoria.

## 10. Cierre operativo del proyecto
- [x] Definir una ruta oficial de ejecucion desde cero.
- [ ] Probar esa ruta en limpio en la maquina final.
- [x] Resolver incoherencias entre Docker, scripts standalone y cluster de 3 VMs dejando Docker/local como ruta oficial.
- [x] Confirmar que todos los comandos del README funcionan o actualizarlos.
- [ ] Dejar una checklist de entrega final marcada con fecha.
- [ ] Validar visualmente el dashboard final y que la pestaña de stock/Gantt se ve correctamente.

## 11. Checklist de entrega final
- [x] Stack final levantado sin errores.
- [x] Ingesta real demostrable desde API/NI-Fi/Kafka.
- [x] Copia raw en HDFS demostrable.
- [x] Staging y curated en Hive demostrables.
- [x] Grafo y metricas defendibles demostrables.
- [x] Persistencia Cassandra demostrable para baja latencia.
- [x] DAG Airflow ejecutado con exito y capturado.
- [x] Al menos una alerta operativa generada y registrada.
- [x] Rebuild tecnico reproducible tras reinicio validado.
- [x] Dashboard final ensayado y legible en exposicion.
- [ ] Memoria completa con capturas.
- [ ] Demo final ensayada.
