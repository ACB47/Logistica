# Documento Word (guion + capturas)

Este fichero es el **esqueleto** del documento Word. La idea es escribir aquí primero (con imágenes/capturas referenciadas) y luego exportar a `.docx`.

---

## 0. Portada

- Título: *Plataforma Big Data para logística marítima y cadena de suministro (Valladolid)*
- Autor / asignatura / fecha
- Entorno: **SER 5 AMD Ryzen 7 5700**, 12 GB RAM, VirtualBox (3 VMs: master + 2 slaves)

---

## 1. Contexto y problema (KDD – Selección)

### 1.1 Sector y caso de uso

- **Cadena de suministro**: importación marítima desde Shanghai a España, con entrega final en Valladolid.
- **Puntos críticos**:
  - stock del almacén (riesgo de ruptura),
  - tiempos de tránsito puerto→Valladolid,
  - tiempos de descarga/gestión/preparación,
  - riesgo externo (geopolítica, clima).

### 1.2 Objetivo del sistema

- Monitorizar barcos por GPS.
- Cruzar esa señal con alertas globales.
- Generar **alertas accionables** (plan de contingencia: vuelo urgente).

### 1.3 Justificación de decisiones (importante para la rúbrica)

- Kafka para desacoplar fuentes y consumidores.
- HDFS para auditoría y escalabilidad (landing raw).
- Spark SQL para limpieza/normalización y joins.
- GraphFrames para modelar rutas y criticidad (grafos).
- Hive como capa SQL histórica/curada (sin Cassandra por restricción).
- Airflow para orquestación y reintentos/alertas.

**Captura obligatoria**: diagrama de arquitectura (lo haremos en la sección 2).

---

## 2. Arquitectura final (KDD – Selección + Preprocesamiento)

### 2.1 Arquitectura (Lambda/Kappa adaptada)

El enunciado pide Lambda/Kappa y streaming; por restricción del proyecto **no usamos Structured Streaming**.

**Solución equivalente**:
- Ingesta en Kafka (topics `datos_crudos`, `alertas_globales`).
- Persistencia **raw** en HDFS (paths del enunciado).
- Procesamiento por **micro‑lotes** desde HDFS con Spark (cada N minutos).
- Publicación de “resultados” (tablas curadas en Hive + alertas finales en Kafka si aplica).

### 2.2 Componentes (qué va en cada VM)

Tabla a completar:
- `master`: NameNode, ResourceManager, Hive Metastore/HiveServer2 (si aplica), Airflow, Kafka controller/broker (KRaft), NiFi (si aplica)
- `slave01`: DataNode, NodeManager, Kafka broker (KRaft)
- `slave02`: DataNode, NodeManager, Kafka broker (KRaft)

**Capturas**:
- VirtualBox: 3 VMs creadas, RAM/CPU asignada.
- `jps`/servicios Hadoop activos en master y slaves.

---

## 3. Preparación del entorno (VirtualBox + Hadoop)

### 3.1 Recursos y red

- CPU/RAM por VM (justificación: 12 GB total).
- Red: NAT + Host‑Only (o puente) y resolución de nombres (`/etc/hosts`).

**Capturas**:
- Configuración de red por VM.
- `ping` entre `master`, `slave01`, `slave02`.

### 3.2 HDFS + YARN

- Formateo y arranque.
- Validación con `hdfs dfs -ls /`.
- Validación YARN UI.

**Capturas**:
- HDFS UI (NameNode).
- YARN UI (ResourceManager).

---

## 4. Ingesta (KDD – Ingesta y Selección): Kafka KRaft + “landing raw” HDFS

### 4.1 Topics Kafka

- `datos_crudos`: GPS barcos, eventos operativos (puerto, almacén, stock).
- `alertas_globales`: noticias geopolíticas y clima (y/o eventos agregados).

**Capturas**:
- Comandos de creación de topics.
- `kafka-topics --describe`.

### 4.2 Formato de eventos (contratos)

Definir JSON mínimo:
- **ShipPositionEvent**: `ship_id`, `ts`, `lat`, `lon`, `speed`, `heading`, `route_id`, `eta_port`
- **AlertEvent**: `source` (clima/noticias), `ts`, `severity`, `region`, `text`, `confidence`
- **StockEvent**: `sku`, `warehouse` (Valladolid), `ts`, `stock_on_hand`, `reorder_point`

**Justificación**: contratos estables para desacoplar.

### 4.3 Landing raw en HDFS

Rutas:
- `/hadoop/logistica/raw/ships`
- `/user/logistica/raw/opensky`
- `/hadoop/logistica/raw/noticias`
- `/hadoop/logistica/raw/clima`

**Capturas**:
- `hdfs dfs -mkdir -p ...`
- `hdfs dfs -ls -R /hadoop/logistica/raw`

> Nota: cómo se hace la copia “raw” depende de la herramienta final (NiFi/consumidores). Se documenta el mecanismo elegido.

---

## 5. Preprocesamiento y transformación (KDD – Preprocesamiento/Transformación): Spark SQL + Hive

### 5.1 Limpieza y normalización

- Parsing JSON → columnas tipadas.
- Control de nulos y duplicados.
- Normalización de timestamps/zonas horarias.

**Capturas**:
- Ejecución de un job Spark en YARN.
- Tabla staging en Hive.

### 5.2 Enriquecimiento con datos maestros (Hive)

Tablas maestras (ejemplos):
- `dim_ports` (Shanghai, Algeciras/Valencia/Barcelona)
- `dim_warehouse` (Valladolid)
- `dim_routes` (rutas marítimas y puerto→Valladolid)
- `dim_skus` (lead time, criticidad)

**Justificación**: joins reproducibles, gobernanza SQL.

---

## 6. Análisis de grafos (KDD – Transformación/Minería): GraphFrames

### 6.1 Modelo de grafo

- **Nodos**: puertos, almacén Valladolid, nodos logísticos intermedios (si aplica).
- **Aristas**: rutas con pesos (tiempo estimado, riesgo).

### 6.2 Métricas

- Camino más corto/rápido (peso = tiempo).
- Camino “más seguro” (peso = riesgo compuesto).
- Identificación de nodos críticos (centralidad aprox / comunidades si aplica).

**Capturas**:
- Resultado de rutas recomendadas.
- Tabla Hive `fact_route_risk`.

---

## 7. Minería y acción (KDD – Minería/Interpretación): reglas + scoring de riesgo

### 7.1 Scoring de riesgo (sin streaming estructurado)

Riesgo compuesto ejemplo:
- clima (severidad),
- geopolítica (severidad),
- retraso estimado (vs ETA),
- stock (si bajo reorder point).

Salida:
- **alerta_operativa** con recomendación: “activar vuelo urgente” si \(riesgo\) alto y stock crítico.

**Capturas**:
- Ejemplos de alertas generadas.
- Registro en Hive `fact_alerts`.

---

## 8. Orquestación (KDD – Interpretación/Acción): Airflow

### 8.1 DAG principal

Tareas típicas:
- `landing_to_staging_ships`
- `landing_to_staging_alerts`
- `build_dims`
- `build_graph_metrics`
- `compute_risk_and_alerts`

Requisitos rúbrica:
- reintentos,
- dependencias claras,
- alertas (email o log) en fallo.

**Capturas**:
- Vista del DAG.
- Ejecución correcta (verde) + ejemplo de reintento.

---

## 9. Evidencias y capturas (checklist)

- VirtualBox: 3 VMs (nombres correctos).
- Red: ping + /etc/hosts.
- HDFS: UIs + `hdfs dfs -ls`.
- YARN: UI + Spark job en cluster.
- Kafka KRaft: topics creados + describe.
- Hive: tablas raw/staging/curated.
- GraphFrames: salida (rutas/riesgo).
- Airflow: DAG + runs.

---

## 10. Conclusiones

- Qué problemas resuelve.
- Limitaciones (no Cassandra, no structured streaming).
- Mejoras futuras (observabilidad, modelos ML más avanzados, etc.).

