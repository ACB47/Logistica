# SESSION_STATUS.md

Estado rapido del proyecto para poder retomar la sesion sin reanalizar todo el repo.

## Ultima actualizacion
- Fecha de referencia: 2026-04-21
- Estado general: documentacion final cerrada, capturas completas, dashboard y memoria alineados con la ruta Docker/local.
- Sincronizacion documental mas reciente: `docs/00_documento_word.docx` pasa a ser la fuente de verdad y se han alineado `docs/00_documento_word.md` y `docs/00_documento_word.odt` con los ultimos cambios de contenido.
- Ajuste funcional reciente en `dashboard/app.py`: recuperadas las vistas de `Control Tower` y `Alertas y Contingencias` cuando `fact_air_recovery_options` llega vacia, mediante fallback demo derivado de `ships_latest` y protecciones frente a columnas ausentes.

## Resumen ejecutivo
- El proyecto queda defendible de extremo a extremo sobre una arquitectura Docker/local con NiFi, Kafka, HDFS, Spark, Hive, GraphFrames, Cassandra, Airflow y Streamlit.
- La memoria principal ya esta consolidada en:
  - `docs/00_documento_word.md`
  - `docs/00_documento_word.odt`
  - `docs/2026_PredictiveLogistics.pdf`
- Las capturas de evidencia necesarias para la entrega ya estan recopiladas en `Capturas de pantalla/`.
- El dashboard Streamlit ya refleja el estado final de la demo:
  - `Control Tower Valladolid` simplificado
  - mapa maritimo con barcos sobre corredores reales
  - tabla ETA con nombres reales de barcos
  - fallback del bundle y fallback demo para alertas SMTP

## Estado funcional por bloques

### Ingesta
- Kafka KRaft operativo en Docker.
- Topics principales validados:
  - `datos_crudos`
  - `alertas_globales`
  - `datos_filtrados`
  - `datos_filtrados_ok`
- Flujo NiFi con Open-Meteo documentado y con evidencias visuales en `docs/nifi/OpenMeteo_Kafka_Flow.json` y `Capturas de pantalla/`.
- Landing raw en HDFS validado para `ships`, `clima` y `noticias`.

### Spark / Hive / GraphFrames
- Jobs principales implementados y documentados.
- Staging, dimensiones y facts validadas en Hive.
- Caso de grafos defendible con `fact_graph_hops` y `fact_graph_centrality`.
- Regla operativa confirmada: ejecutar `spark-sql` y `spark-submit` con Hive de forma secuencial para evitar bloqueos Derby.

### Cassandra
- `logistica.vehicle_latest_state` validada como capa de consulta rapida.

### Airflow
- DAGs visibles y documentados:
  - `logistica_kdd_microbatch`
  - `logistica_kdd_monthly_retrain`
- Hay evidencia de run exitoso y reejecucion controlada en capturas.

### Dashboard
- El bundle del dashboard esta endurecido con fallback a `jobs/dashboard_bundle_output.last_good.json`.
- El job `jobs/spark/99_dashboard_bundle.py` ya asigna `ship_name` a todos los barcos del bundle.
- `start.sh` ofrece opcion `3) Dashboard solo (ultraligero)`.
- El sidebar incluye enlace tecnico a Zeppelin en `http://localhost:8081`.
- `Alertas y Contingencias` y la proyeccion de stock siguen mostrando datos aunque falte `fact_air_recovery_options` en el bundle.

## Documentacion final
- La memoria PDF final a revisar/entregar es `docs/2026_PredictiveLogistics.pdf`.
- La version editable mas reciente es `docs/00_documento_word.docx`.
- El editable principal es `docs/00_documento_word.odt`.
- Se guardo copia previa del ODT antes de la limpieza final en:
  - `docs/00_documento_word.pre_fix.bak.odt`

## Capturas y evidencias
- La carpeta `Capturas de pantalla/` ya cubre arquitectura, servicios, Kafka, NiFi, HDFS, Spark, Hive, GraphFrames, Cassandra, Airflow y dashboard.
- No quedan pendientes reales de capturas obligatorias para la memoria.
- La reejecucion de Airflow queda como evidencia extra ya disponible, no como requisito pendiente.

## Git
- Los ultimos commits relevantes dejaron cerrados:
  - documentacion de entrega
  - evidencias visuales
  - ajuste final del documento
- La rama `main` ya fue actualizada en remoto durante esta sesion.

## Si se retoma otra sesion
1. Revisar visualmente `docs/00_documento_word.odt` y `docs/2026_PredictiveLogistics.pdf` por si se quieren ultimos ajustes cosmeticos.
2. Si hace falta rehacer demo tecnica, levantar servicios con `docker compose up -d` o usar `./start.sh` para modo ultraligero del dashboard.
3. Si el dashboard aparece vacio, regenerar bundle y comprobar HDFS antes de tocar la UI.
