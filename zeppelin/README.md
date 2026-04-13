# Notebooks Zeppelin

Estos notebooks están diseñados para análisis interactivo en Zeppelin.

---

## Notebook 01: Overview Ejecutivo

**Propósito**: Resumen ejecutivo del pipeline y estado actual.

**Paragraphs**:
1. Resumen del Pipeline - Lista todas las tablas Hive
2. Estadisticas de Ships - Conteo y distribución por destino
3. Alertas Climaticas - Resumen de alertas de clima
4. Alertas Noticias - Resumen de alertas geopolíticas
5. Stock Valladolid - Artículos en stock
6. Pedidos Douai - Pedidos de clientes Francia

---

## Notebook 02: Análisis de Rutas

**Propósito**: Análisis detallado de rutas marítimas.

**Paragraphs**:
1. Barcos por Destino - Distribución de barcos por puerto
2. Rutas Disponibles - Catálogo de rutas
3. Puertos - Catálogo de puertos
4. Tiempos de Transito - Tiempos estimados por ruta

---

## Notebook 03: Análisis de Alertas

**Propósito**: Análisis de alertas y riesgos.

**Paragraphs**:
1. Resumen de Alertas - Aggregaciones por riesgo/stock
2. Alertas Criticas - severity >= 4
3. Opciones de Contingencia - Recuperación aérea
4. Por Region - Distribución geográfica

---

## Notebook 04: Modelos ML y Grafos

**Propósito**: Análisis avanzado con ML y grafos.

**Paragraphs**:
1. Modelos de ML - Librerías disponibles
2. Grafos - Nodos críticos
3. Grafos - Métricas de rutas
4. Facts Operativos - Datos operativos

---

## Uso en Zeppelin

```bash
# Zeppelin disponible en:
http://localhost:8082

# Importar notebooks
# 1. Copiar archivos .json a Zeppelin notebooks/
# 2. O usar API REST de Zeppelin
```

---

## Capturas para Memoria

Cada notebook debe ser ejecutado y capturado para la memoria:
- 01_overview.png
- 02_routes.png
- 03_alerts.png
- 04_ml.png