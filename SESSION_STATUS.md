# SESSION_STATUS.md

Estado rapido del proyecto para poder retomar la sesion sin reanalizar todo el repo.

## Ultima actualizacion
- Fecha de referencia: 2026-04-12
- Contexto: refactor del sidebar Streamlit con navegación por botones y ajuste de la regla económica visible en la portada ejecutiva.

## Resumen ejecutivo
- El proyecto ya tiene una base funcional de demo: productores Kafka, landing raw en HDFS, jobs Spark batch, tablas Hive, soporte Cassandra, notebooks Zeppelin y un DAG de Airflow.
- El proyecto aun no esta cerrado respecto al enunciado final 2025-2026.
- Ya se inicio la alineacion del stack Docker: Kafka paso a KRaft en Compose, NiFi subio a `2.6.0`, Cassandra a `5.0`, y la ruta oficial de entrega queda fijada en Docker/local.
- NiFi si esta presente en Docker y ahora hay un script de gestion por perfiles para evitar levantar todo el stack cuando no hace falta: `scripts/60_manage_docker_stack.sh`.
- Se probo `simple core`, pero el arranque quedo bloqueado por falta de espacio en disco en la maquina host.
- Se libero espacio en disco eliminando cache Docker, cache de `pip` y backends pesados de LM Studio.
- Se reprobo el arranque `simple core` y esta operativo: Kafka KRaft, Postgres, Airflow y NiFi levantan correctamente.
- La inicializacion de Kafka ya crea los tres topics base del proyecto: `datos_crudos`, `alertas_globales` y `datos_filtrados`.
- Se dejo preparada la base del primer flujo real de NiFi con Open-Meteo, credenciales fijas locales y script de healthcheck autenticado.
- El flujo real de NiFi ya quedo validado extremo a extremo con mensajes visibles en Kafka tanto en `datos_crudos` como en `datos_filtrados`.
- Ya existe un job Spark dedicado para mover `datos_filtrados` a Hive staging: `jobs/spark/01_weather_filtered_to_staging.py`.
- El flujo de NiFi ya esta exportado para reutilizarlo y documentarlo en `docs/nifi/OpenMeteo_Kafka_Flow.json`.
- Queda preparado un wrapper para lanzar el job de weather staging: `scripts/63_run_weather_filtered_staging.sh`.
- Ya se probaron varias veces los pasos reales de Spark/Hive; el bloqueo actual ya no es Kafka ni permisos base, sino HDFS sin DataNode utilizable para escritura.
- El despliegue HDFS Docker ya fue corregido: NameNode y DataNode registran correctamente con `fs.defaultFS=hdfs://namenode:8020` y volumenes separados.
- El flujo completo `datos_filtrados -> Spark -> Hive/HDFS` ya esta validado con 3 filas reales cargadas en `logistica.stg_weather_open_meteo`.
- Ya existe una segunda capa KDD sobre el clima: `logistica.dim_ports_routes_weather`, que enriquece `stg_weather_open_meteo` con contexto de ruta, almacen y estado operativo.
- Ya existe una fact table operativa final: `logistica.fact_weather_operational`, con accion recomendada y severidad operativa para la toma de decisiones.
- Los jobs legacy de staging, grafos y alertas ya vuelven a funcionar con HDFS explicito en `hdfs://namenode:8020`.
- Ya existen dimensiones maestras reales en Hive para puertos, rutas, almacen y SKU: `dim_ports`, `dim_routes`, `dim_warehouse`, `dim_skus`.
- GraphFrames ya cubre una segunda metrica defendible: criticidad por grado en `logistica.fact_graph_centrality`.
- El caso Cassandra de baja latencia ya queda cubierto con `logistica.vehicle_latest_state` y el loader `scripts/65_load_vehicle_latest_state_cassandra.sh`.
- El pipeline streaming ya queda alineado al enunciado en ventanas de `15 minutes` y con checkpoints HDFS explicitos.
- La estrategia de defensa ya queda decidida: `micro-batch documentado` como camino principal, con streaming real como evidencia complementaria.
- Se detecto que la perdida intermitente de tablas Hive no era Cassandra sino el metastore local de Spark; ya queda mitigado con volumen persistente en `docker-compose*.yml` y un rebuild rapido en `scripts/66_rebuild_hive_demo_tables.sh`.
- El error concreto de Derby (`metastore_db already exists`) queda corregido moviendo la persistencia a `derby.system.home=/home/jovyan/.derby` en el servicio `spark`.
- `datos_filtrados_ok` ya queda incluido en la inicializacion automatica de Kafka para evitar fallos del rebuild tras reinicios.
- El rebuild completo ya queda validado de nuevo: `fact_alerts` y el resto de tablas principales reaparecen correctamente tras `scripts/66_rebuild_hive_demo_tables.sh`.
- Ya existe una base de dashboard en `Streamlit` con mapa, diagramas, estado de servicios y tablas clave para la defensa.
- La pestaña `KDD Fase I - Ingesta` ya se rearmo para mostrar un mapa operacional de barcos en transito, rutas, puertos y una tabla de ETA con ETA original, ETA recalculada, coste maritimo e icono aereo.
- Se anadio `docker-compose.hdfs.yml` como stack de referencia para ver el dashboard con datos reales, evitando el bundle vacio del stack simple.
- Ya existe en codigo un nuevo caso de uso de contingencia: comparacion `barco vs aereo+camion` hasta Valladolid mediante `jobs/spark/03_air_recovery_options.py`.
- `logistica.fact_air_recovery_options` ya fue generada y consultada con un caso valido de comparacion `barco vs aereo+camion`.
- Regla operativa confirmada: los `spark-submit` y `spark-sql` con Hive deben ejecutarse secuencialmente para evitar bloqueos de Derby en el metastore embebido.
- Ya existe una ampliacion del dashboard para stock de Valladolid, pedidos de Douai y Gantt por semanas industriales apoyada en `dim_articles_valladolid`, `fact_customer_orders_douai` y `fact_article_gantt`.
- El dashboard ya contempla dos clientes franceses (`Douai` y `Cleon`), dos orígenes asiáticos (`Shanghai` y `Yokohama`) y simulación de incidencias por barco con recálculo de ETA y `CUBRE / NO CUBRE`.
- La pestaña de ingesta ya debe enseñar tabla de barcos con ETA en formato fecha, salida desde origen y visualización GPS sobre el corredor marítimo Asia -> España.
- Punto exacto de reanudacion: al volver hay que refrescar el bundle del dashboard con `spark-submit jobs/spark/99_dashboard_bundle.py` y comprobar que el mapa operacional y la tabla de ETA cargan con los filtros de destino/barco.
- Punto exacto de reanudacion: si el dashboard sale vacio, levantar `docker compose -f docker-compose.hdfs.yml up -d` y regenerar `dashboard_bundle_output.json` antes de abrir Streamlit.
- El bundle del dashboard ya incorpora `eta_hours_estimate` por barco para conectar ETA con riesgo de stock y decisiones de contingencia.
- Se corrigió el error de recreación `KeyError: 'ContainerConfig'` eliminando el uso operativo de `docker-compose` v1 y migrando scripts y dashboard a `docker compose` v2.
- El stack completo ahora arranca con límites de memoria/CPU y `healthcheck` en Kafka, HDFS y Cassandra para reducir saturación del equipo y evitar dependencias prematuras.
- Se añadió `jobs/spark/spark_config.py` como configuración compartida de Spark local con menos memoria y menos particiones shuffle.
- La portada `Resumen Ejecutivo` del dashboard fue rediseñada con KPIs gerenciales, donuts operativos, área de proyección y panel ROI de contingencias con fallback visual para demo.
- El panel de servicios del dashboard ya detecta correctamente los nombres actuales de contenedores de Docker Compose v2 (`logistica-*-1`).
- El sidebar ya no usa `radio`; ahora navega con botones homogéneos apoyados en `st.session_state["vista_actual"]`.
- Se eliminó del sidebar la sección de parámetros de demo (`Factor de demanda`, `Retraso ETA`, `Semana industrial`, `Cliente destino`) para simplificar la defensa y mantener foco ejecutivo.
- La regla económica de contingencia aérea quedó explicitada en la portada: activar solo si el ahorro estimado es `>= 24h` y la inversión media `<= 18.000 EUR`.
- Lo mas importante pendiente ahora es cerrar evidencias, Airflow visual, narrativa final de defensa y documentacion completa sobre la ruta Docker/local.
- Se añadieron nuevas pestañas al dashboard Streamlit:
  - `10. Alertas y Contingencias`: Panel de alertas críticas (fact_alerts), tabla interactiva de decisión (fact_air_recovery_options), scatter plot de IA (coste vs tiempo), mapa de contingencia multimodal con rutas marítimas y desviaciones aéreas.
  - `11. Ejecución Contingencia Multimodal`: Evaluación de alternativas aéreas con compañías y números de vuelo, referencias críticas a volar, alternativas de última milla con matriculas de camiones, mapa de seguimiento multimodal en tiempo real.
  - `KDD Fase II - Spark & MLlib` (mejora): Simulador de streaming con micro-batch de 15 min, K-Means (clustering puertos), RandomForest (feature importance alertas), LinearRegression (predicción ETA).
  - `Análisis de Red (GraphFrames)` (mejora): Fórmula de pesos dinámicos P=T+(1/F)+Ra, visualización de grafo con nodos Asia/España/Francia, panel de nodos críticos (fact_graph_centrality), panel de riesgo de rutas (fact_route_risk).
- El contenido de 'Impacto en Cliente' se integro dentro de la pestaña 'Control Tower Valladolid' para mejor cohesion.
- Barcos con nombres reales y navieras: MSC Gülsün (MSC), CMA CGM Jacques Saade (CMA CGM), Ever Golden (Evergreen), ONE Apus (ONE), Maersk Eindhoven (Maersk).
- Coordenadas GPS de barcos en Hive actualizadas a corredores marítimos reales (Mar Arábigo, Índico, Mediterráneo).
- Leyendas de graficos corregidas para mostrar nombres legibles en espanol.
- Mejoras adicionales en el dashboard:
  - `Control Tower Valladolid`: Warehouse dashboard con donut de capacidad y grafico de stock por categoria, seguimiento de flota con nombres reales de barcos, barra de progreso de trayecto y tabla de contingencia con estado CUBRE/NO CUBRE.
  - `7. Persistencia`: KPIs (HDFS 2.4TB, 18 tablas Hive, 12ms latencia Cassandra), grafico de flujo de registros, donut de data lake y alertas, catalogo de 12 tablas, mapa de linaje Graphviz.
  - `8. Orquestacion`: Panel de salud, timeline de DAG con px.timeline, donut de tasa de exito, historial de ejecuciones, enlace a consola Airflow.
   - `9. Evidencias KDD`: KPIs de resumen (5 fases, 3 modelos, 4 formatos), linea visual del flujo KDD, selector de diagramas UML, export NiFi JSON, simulacion HDFS ls.

## Recientes mejoras (2026-04-09)
- **Control Tower Valladolid**: Refactorizado con 3 bloques verticales obligatorios:
  1. Tabla de detalles de almacén (dim_articles_valladolid)
  2. Gráfico de barras de stock por referencia
  3. Horizonte de 10 semanas industriales (Gantt)
- **Evidencias KDD**: Reformateado como panel de auditoría académica con 4 bloques:
  1. Panel de Modelos de IA y KDD (KPIs): Fases KDD 5/5, MLlib 3 algoritmos, GraphFrames 156 nodos
  2. Arquitectura Lambda visual: Diagrama Open-Meteo → NiFi → Kafka → Spark → HDFS/Hive/Cassandra → Dashboard
  3. Visor UML con tabs: Casos de Uso, Diagrama de Clases, Diagrama de Secuencia, Arquitectura
  4. Auditoría Spark/NiFi: Petición GET a localhost:8080, tabla de aplicaciones, JSON de flujo NiFi
- Eliminada tabla de stock duplicada que estaba en Evidencias KDD (pertenece a Control Tower)

## Recientes mejoras (2026-04-11)
- **Servicios Docker**:
  1. Migración de scripts operativos y dashboard a `docker compose`
  2. Eliminación del warning por `version` obsoleta en `docker-compose.yml`
  3. Límites de recursos para servicios pesados y arranque ordenado con `healthcheck`
- **Spark local**:
  1. Nueva configuración compartida en `jobs/spark/spark_config.py`
  2. Ajuste de memoria y `spark.sql.shuffle.partitions` para entorno local
- **Dashboard Streamlit**:
  1. Rediseño de `Resumen Ejecutivo`
  2. Fix del panel de estado de servicios con nombres Compose v2
  3. Fallbacks de datos para demos sin conexión completa

## Recientes mejoras (2026-04-12)
- **Sidebar corporativo**:
  1. Cabecera institucional `CONTROL TOWER ANACO`
  2. Navegación agrupada por bloques con botones de ancho uniforme
  3. Acciones globales destacadas
  4. Estado del stack y enlaces técnicos dentro de un expander final

## Estado por fases KDD

### Fase 0 - Base del repositorio
- Estado: avanzada.
- Hecho:
  - `AGENTS.md` creado y actualizado.
  - `TODO.md` creado con checklist completo.
  - Estructura principal del repo identificada.
  - `SESSION_STATUS.md` ya sirve como punto de reanudacion.
- Pendiente:
  - Mantener `SESSION_STATUS.md` al final de cada bloque de trabajo relevante.

### Fase I - Ingesta y seleccion
- Estado: parcial pero funcional.
- Hecho:
  - Productor GPS: `ingesta/productores/ships_gps_producer.py`
  - Productor alertas: `ingesta/productores/alerts_producer.py`
  - Landing raw Kafka -> HDFS: `ingesta/consumidores/kafka_to_hdfs_raw.py`
  - Topics actuales: `datos_crudos`, `alertas_globales`
  - Kafka Docker alineado a KRaft en `docker-compose.yml` y `docker-compose.simple.yml`
  - NiFi Docker alineado a `2.6.0`
  - Script de gestion Docker por perfiles: `scripts/60_manage_docker_stack.sh`
  - Comprobado que NiFi esta declarado en Docker, pero no arranca mientras no se libere espacio en disco
  - Verificado acceso HTTPs a NiFi con respuesta `302` en `https://localhost:8443`
  - Kafka crea correctamente `datos_crudos`, `alertas_globales` y `datos_filtrados` en el arranque ligero
  - NiFi ya usa credenciales locales definidas por entorno y healthcheck funcional
  - Documentado primer flujo real en `docs/01_nifi_open_meteo_flow.md`
  - Script de verificacion NiFi: `scripts/61_nifi_healthcheck.sh`
  - Flujo Open-Meteo validado con consumo real en Kafka:
    - `datos_crudos` contiene el JSON original de la API
    - `datos_filtrados` contiene el JSON transformado para analitica
  - Export del canvas NiFi disponible en `docs/nifi/OpenMeteo_Kafka_Flow.json`
  - Stack `simple core` reanudado con Kafka, NiFi y Postgres activos; Airflow vuelve a levantar tras recrear el contenedor
- Falta para cerrar:
  - evidencias finales de auditoria raw

### Fase II - Preprocesamiento y transformacion
- Estado: bastante avanzada.
- Hecho:
  - staging en Hive/HDFS con `jobs/spark/01_raw_to_staging.py`
  - nuevo staging desde Kafka `datos_filtrados` a Hive con `jobs/spark/01_weather_filtered_to_staging.py`
  - limpieza, `dropna`, `dropDuplicates`, schemas explicitos
  - analitica de grafos con `jobs/spark/02_graph_metrics.py`
  - `01_weather_filtered_to_staging.py` ya incorpora `fs.defaultFS` hacia `hdfs://namenode:8020`
  - se creo `/user/hive/warehouse` y `/hadoop/logistica/staging` con permisos de escritura en HDFS
  - `spark-submit` del job ya funciona con el paquete Kafka correcto y crea `logistica.stg_weather_open_meteo`
  - tabla validada con datos reales:
    - `2026-03-26T19:00:00 | 15.1 | 75 | 38.7 | severity 3`
    - `2026-03-26T18:55:00 | 14.5 | 78 | 22.1 | severity 2`
    - `2026-03-26T18:50:00 | 14.2 | 80 | 12.4 | severity 1`
  - enriquecimiento validado en `logistica.dim_ports_routes_weather`:
    - `Algeciras | Shanghai | route-shanghai-algeciras | Valladolid | severity 3 | MEDIO | METEO_VIGILANCIA | 6.0h`
    - `Algeciras | Shanghai | route-shanghai-algeciras | Valladolid | severity 2 | MEDIO | OPERATIVO | 3.0h`
    - `Algeciras | Shanghai | route-shanghai-algeciras | Valladolid | severity 1 | BAJO | OPERATIVO | 1.0h`
  - fact operativa validada en `logistica.fact_weather_operational`:
    - `Algeciras | route-shanghai-algeciras | MEDIO | METEO_VIGILANCIA | 6.0h | reprogramar ventana portuaria... | 4`
    - `Algeciras | route-shanghai-algeciras | MEDIO | OPERATIVO | 3.0h | seguimiento normal... | 3`
    - `Algeciras | route-shanghai-algeciras | BAJO | OPERATIVO | 1.0h | seguimiento normal... | 2`
  - pipeline legacy restaurado con muestra raw en HDFS:
    - `logistica.stg_ships`: 3 filas
    - `logistica.stg_alerts_clima`: 2 filas
    - `logistica.stg_alerts_noticias`: 2 filas
    - `logistica.fact_route_risk`: 3 filas
    - `logistica.fact_graph_hops`: 5 nodos
    - `logistica.fact_alerts`: 3 filas
  - dimensiones maestras validadas:
    - `logistica.dim_ports`: 4 puertos
    - `logistica.dim_routes`: 3 rutas
    - `logistica.dim_warehouse`: 1 almacen
    - `logistica.dim_skus`: 1 SKU
  - enriquecimiento ya usa dimensiones Hive reales:
    - `PORT-ALG | Algeciras | route-shanghai-algeciras | WH-VLL | Valladolid | MEDIO | METEO_VIGILANCIA`
    - `PORT-ALG | route-shanghai-algeciras | reprogramar ventana portuaria... | 4`
  - segunda metrica de grafos validada en `logistica.fact_graph_centrality`:
    - `Shanghai | degree 3 | NODO_CRITICO`
    - `Valladolid | degree 3 | NODO_CRITICO`
    - `Algeciras | degree 2 | NODO_CRITICO`
- Falta para cerrar:
  - mejor cierre del caso de grafos para la defensa
  - terminar de preparar o reutilizar `.venv-jobs` para ejecucion local con PySpark si se quiere correr fuera del cluster

### Fase III - Mineria y accion
- Estado: parcial.
- Hecho:
  - scoring y alertas batch: `jobs/spark/03_score_and_alert.py`
  - pipeline streaming/ML experimental: `jobs/spark/04_streaming_ml_pipeline.py`
  - modulo email: `jobs/spark/05_email_alerts.py`
  - Cassandra Docker subida a `5.0` en Compose
  - tabla Cassandra `logistica.vehicle_latest_state` validada con 3 vehiculos:
    - `ship-001 | Algeciras | stock 11 | reorder 30`
    - `ship-002 | Valencia | stock 93 | reorder 30`
    - `ship-003 | Barcelona | stock 95 | reorder 30`
  - `jobs/spark/04_streaming_ml_pipeline.py` actualizado a ventanas de `15 minutes`
  - checkpoints streaming alineados a `hdfs://namenode:8020/hadoop/logistica/checkpoint/...`
  - modelado un caso de recuperacion aerea con comparacion de ETA, coste total y riesgo de stock
  - modelado un panel de stock para Valladolid y pedidos de Douai con planificacion semanal
- Falta para cerrar:
  - integracion completa de alertas y pruebas de extremo a extremo
  - integrar visualmente `logistica.fact_air_recovery_options` en la demo final y en el dashboard
  - validar visualmente el Gantt y el panel de stock en el dashboard

### Fase IV - Orquestacion
- Estado: parcial y utilizable.
- Hecho:
  - DAG existente: `airflow/dags/logistica_kdd_dag.py`
  - dependencias y reintentos basicos
  - DAG ya separado en tareas mas cercanas al KDD real: staging, dimensiones, grafo, scoring, weather facts, Cassandra y limpieza HDFS
  - reentrenamiento mensual y limpieza HDFS ya modelados en un segundo DAG: `logistica_kdd_monthly_retrain`
  - visibilidad de fallo mejorada con `email_on_failure`
- Falta para cerrar:
  - validacion en la version final de Airflow de la entrega
  - preparar evidencias visuales: Graph view, run exitoso y reintento

### Fase V - Documentacion y defensa
- Estado: empezada pero no cerrada.
- Hecho:
  - guia base: `docs/00_documento_word.md`
  - notebooks Zeppelin existentes en `zeppelin/`
  - checklist exacto de evidencias y comandos ya documentado en `docs/00_documento_word.md`
  - ya existe un guion formal de defensa documentado en `docs/00_documento_word.md`
  - ya hay evidencias manuales empezadas de NiFi, Kafka y Hive weather staging
  - capturas ya realizadas en esta tanda:
    - NiFi canvas + `InvokeHTTP` + `PublishKafka`
    - Kafka `datos_filtrados`
    - Hive `stg_weather_open_meteo`
    - Hive `dim_ports_routes_weather`
    - Hive `fact_weather_operational`
    - Hive `fact_alerts`
    - Cassandra `vehicle_latest_state`
    - HDFS curated
    - GraphFrames `fact_graph_centrality`
    - Airflow: lista de DAGs + graph de `logistica_kdd_microbatch` + graph de `logistica_kdd_monthly_retrain`
- Falta para cerrar:
  - memoria final completa
  - narrativa clara de arquitectura final y limitaciones
  - guion de demo final

## Desajustes importantes detectados contra el enunciado
- Kafka Docker ya esta pasado a KRaft, pero falta validarlo en una ejecucion real del stack.
- NiFi en Docker ya esta alineado a la version objetivo, pero sigue faltando un flujo real configurado.
- Airflow en Docker principal no esta alineado claramente con 2.10.x.
- Cassandra Docker ya esta en 5.0; falta validar compatibilidad en una ejecucion real.
- Falta separar `datos_crudos` y `datos_filtrados`.
- El job de streaming usa ventanas de 5 minutos, no 15.
- En Docker full, HDFS ya escribe correctamente; ahora el punto a vigilar es no perder mensajes de Kafka al recrear el broker durante pruebas.
- El bloqueo por espacio en disco ya no aplica: `/` volvio a tener margen libre suficiente tras la limpieza.

## Decision de arquitectura actual
- Ruta oficial de entrega: Docker/local con HDFS + Kafka KRaft + Spark + Hive + Cassandra + Airflow + NiFi.
- `docker-compose.yml`: stack completo para la demo final.
- `docker-compose.simple.yml`: validacion rapida y sesiones ligeras.
- No se usaran maquinas virtuales en la entrega final.
- Esta decision ya esta reflejada tambien en `README.md`.

## Archivos clave para retomar
- `TODO.md`
- `AGENTS.md`
- `SESSION_STATUS.md`
- `scripts/60_manage_docker_stack.sh`
- `scripts/61_nifi_healthcheck.sh`
- `scripts/63_run_weather_filtered_staging.sh`
- `scripts/65_load_vehicle_latest_state_cassandra.sh`
- `docs/00_documento_word.md`
- `dashboard/app.py`
- `jobs/spark/99_dashboard_bundle.py`
- `scripts/67_run_dashboard.sh`
- `docs/01_nifi_open_meteo_flow.md`
- `airflow/dags/logistica_kdd_dag.py`
- `jobs/spark/01_raw_to_staging.py`
- `jobs/spark/01_load_master_dimensions.py`
- `jobs/spark/01_weather_filtered_to_staging.py`
- `jobs/spark/02_weather_port_enrichment.py`
- `jobs/spark/03_weather_operational_fact.py`
- `jobs/spark/02_graph_metrics.py`
- `jobs/spark/03_score_and_alert.py`
- `jobs/spark/04_streaming_ml_pipeline.py`
- `ingesta/consumidores/kafka_to_hdfs_raw.py`
- `docker-compose.yml`
- `docker-compose.simple.yml`

## Siguiente bloque recomendado
1. Redactar la memoria final con las capturas ya reunidas.
2. Afinar Airflow para cubrir reentrenamiento mensual y alertas de fallo de forma mas defendible.
3. Dejar una ruta oficial de demo desde Docker completamente ensayada.
4. Revisar SMTP y la demostracion visual de alertas/Airflow antes del cierre.
5. Hacer una pasada final de limpieza documental y de entrega.

## Regla de mantenimiento
- Cada vez que se cierre un bloque de trabajo relevante, actualizar este archivo con:
  - que se hizo
  - que queda pendiente
  - en que fase estamos
  - siguiente paso recomendado
