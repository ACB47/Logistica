# Notebooks Zeppelin

Estos notebooks están diseñados para análisis interactivo en Zeppelin.

En el estado actual del proyecto, Zeppelin actúa como **capa técnica opcional de exploración** y apoyo a validaciones rápidas sobre Spark/Hive. La defensa funcional principal y la visualización ejecutiva se realizan en `Streamlit`, mientras que Zeppelin queda como recurso complementario para análisis ad hoc y revisión de resultados intermedios.

---

## Notebook 01: Overview Ejecutivo

**Archivo**: `01_overview.json`

**Propósito**: Resumen técnico-ejecutivo del pipeline y del estado actual de las tablas principales.

**Paragraphs**:
1. Resumen del Pipeline - Lista todas las tablas Hive
2. Estadisticas de Ships - Conteo y distribución por destino
3. Alertas Climaticas - Resumen de alertas de clima
4. Alertas Noticias - Resumen de alertas geopolíticas
5. Stock Valladolid - Artículos en stock
6. Pedidos Douai - Pedidos de clientes Francia

---

## Notebook 02: Análisis de Rutas

**Archivo**: `02_routes_analysis.json`

**Propósito**: Análisis detallado de rutas marítimas y catálogo de puertos/rutas disponibles.

**Paragraphs**:
1. Barcos por Destino - Distribución de barcos por puerto
2. Rutas Disponibles - Catálogo de rutas
3. Puertos - Catálogo de puertos
4. Tiempos de Transito - Tiempos estimados por ruta

---

## Notebook 03: Análisis de Alertas

**Archivo**: `03_alerts_analysis.json`

**Propósito**: Análisis de alertas, criticidad logística y opciones de contingencia.

**Paragraphs**:
1. Resumen de Alertas - Aggregaciones por riesgo/stock
2. Alertas Criticas - severity >= 4
3. Opciones de Contingencia - Recuperación aérea
4. Por Region - Distribución geográfica

---

## Notebook 04: Modelos ML y Grafos

**Archivo**: `04_ml_models.json`

**Propósito**: Análisis avanzado con modelos de `Spark MLlib` y resultados de grafos.

**Paragraphs**:
1. Modelos de ML - Librerías disponibles
2. Grafos - Nodos críticos
3. Grafos - Métricas de rutas
4. Facts Operativos - Datos operativos

---

## Uso en Zeppelin

```bash
# Zeppelin disponible en:
http://localhost:8081

# Importar notebooks
# 1. Levantar el servicio `zeppelin` con Docker Compose
# 2. Abrir la UI en http://localhost:8081
# 3. Importar los archivos `.json` de esta carpeta desde la interfaz
```

Comando de arranque recomendado si solo se quiere la capa técnica de notebooks:

```bash
docker compose up -d zeppelin spark namenode datanode kafka
```

Nota:
- Zeppelin no es obligatorio para la defensa final.
- Su uso es adecuado para exploración técnica, consultas rápidas y apoyo a capturas analíticas complementarias.

---

## Relación con la memoria

Las capturas principales de la memoria final se apoyan en Streamlit, Hive, Airflow, NiFi y Kafka. No se requiere una captura específica de la UI de Zeppelin para cerrar la entrega, aunque puede utilizarse como evidencia técnica adicional si se desea mostrar análisis exploratorio.

Capturas reutilizables del proyecto que encajan con el contenido analítico del notebook `04_ml_models.json`:
- `Capturas de pantalla/24a_Kmeans.png`
- `Capturas de pantalla/24b_RandomForest.png`
- `Capturas de pantalla/24c_RegresionLineal.png`

Capturas reutilizables relacionadas con resultados técnicos que también pueden contrastarse desde Zeppelin:
- `Capturas de pantalla/18_hive_fact_graph_centrality.png`
- `Capturas de pantalla/19_Hive_fact_alerts.png`
- `Capturas de pantalla/20_evidencia de fact_air_recovery_option.png`

Recomendación:
- Mantener Zeppelin documentado como herramienta de apoyo técnico.
- No presentarlo como la interfaz principal del proyecto frente al dashboard `Streamlit`.
