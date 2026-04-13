# Contrato de Eventos por Topic Kafka

Este documento define el esquema de eventos para cada topic Kafka del proyecto.

---

## 1. Topic: `datos_crudos`

**Productor:** `ingesta/productores/ships_gps_producer.py`
**Descripción:** Posiciones GPS de barcos en tránsito maritime.
**Particiones recomendadas:** 3
**Retención:** 7 días

### Esquema JSON

| Campo | Tipo | Descripción | Obligatorio |
|-------|------|------------|-------------|
| `event_type` | string | Valor fijo: `"ship_position"` | Sí |
| `ts` | ISO8601 | Timestamp UTC de la posición | Sí |
| `ship_id` | string | ID único del barco (ej: `ship-001`) | Sí |
| `route_id` | string | ID de la ruta (ej: `route-shanghai-algeciras`) | Sí |
| `origin_port` | string | Puerto de origen | Sí |
| `dest_port` | string | Puerto de destino | Sí |
| `lat` | float | Latitud (6 decimales) | Sí |
| `lon` | float | Longitud (6 decimales) | Sí |
| `speed_kn` | float | Velocidad en nudos | Sí |
| `heading` | float | Rumbo en grados (0-360) | Sí |

### Ejemplo

```json
{
  "event_type": "ship_position",
  "ts": "2026-04-13T10:30:00+00:00",
  "ship_id": "ship-001",
  "route_id": "route-shanghai-algeciras",
  "origin_port": "Shanghai",
  "dest_port": "Algeciras",
  "lat": 31.2304,
  "lon": 121.4737,
  "speed_kn": 19.5,
  "heading": 245.0
}
```

---

## 2. Topic: `alertas_globales`

**Productor:** `ingesta/productores/alerts_producer.py`
**Descripción:** Alertas climáticas y geopolíticas que afectan rutas marítimas.
**Particiones recomendadas:** 1
**Retención:** 30 días

### Esquema JSON

| Campo | Tipo | Descripción | Obligatorio |
|-------|------|------------|-------------|
| `event_type` | string | Valor fijo: `"global_alert"` | Sí |
| `ts` | ISO8601 | Timestamp UTC de la alerta | Sí |
| `source` | string | Origen: `"clima"` o `"noticias"` | Sí |
| `severity` | int | Severidad 1-5 (5=máxima) | Sí |
| `region` | string | Región afectada | Sí |
| `text` | string | Descripción de la alerta | Sí |
| `confidence` | float | Confianza 0.0-1.0 | Sí |

### Ejemplo

```json
{
  "event_type": "global_alert",
  "ts": "2026-04-13T10:30:00+00:00",
  "source": "clima",
  "severity": 4,
  "region": "Golfo de Adén",
  "text": "Viento fuerte",
  "confidence": 0.85
}
```

---

## 3. Topic: `datos_filtrados`

**Productor:** `jobs/spark/01_weather_filtered_to_staging.py`
**Descripción:** Datos depurados y enrichidos listos para staging.
**Particiones recomendadas:** 3
**Retención:** 7 días

### Esquema JSON

| Campo | Tipo | Descripción | Obligatorio |
|-------|------|------------|-------------|
| `event_type` | string | Tipo de evento original | Sí |
| `ts` | ISO8601 | Timestamp original | Sí |
| `ship_id` | string | ID del barco (si aplica) | No |
| `route_id` | string | ID de la ruta (si aplica) | No |
| `lat` | float | Latitud normalizada | Sí |
| `lon` | float | Longitud normalizada | Sí |
| `weather_score` | float | Score 0-100 condiciones climáticas | No |
| `risk_level` | string | `"low"`, `"medium"`, `"high"` | No |

### Ejemplo

```json
{
  "event_type": "ship_position",
  "ts": "2026-04-13T10:30:00+00:00",
  "ship_id": "ship-001",
  "route_id": "route-shanghai-algeciras",
  "lat": 31.2304,
  "lon": 121.4737,
  "weather_score": 85.0,
  "risk_level": "low"
}
```

---

## 4. Datos de Stock (No Kafka - Bundle Dashboard)

**Origen:** Datos maestros desde Hive (`dim_articles_valladolid`)
**Descripción:** Stock actual, niveles mínimos y consumo por referencia.
**Frecuencia:** Actualización diaria vía Spark job.

### Esquema JSON

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `article_ref` | string | Referencia del artículo |
| `total_stock_pieces` | int | Stock total en piezas |
| `safety_stock_min` | int | Stock mínimo de seguridad |
| `daily_consumption_avg` | float | Consumo diario promedio |
| `inbound_port` | string | Puerto de entrada |
| `warehouse` | string | Almacén destino |

### Ejemplo

```json
{
  "article_ref": "SKU-SEA-001",
  "total_stock_pieces": 150,
  "safety_stock_min": 30,
  "daily_consumption_avg": 12.5,
  "inbound_port": "Algeciras",
  "warehouse": "Valladolid"
}
```

---

## 5. Verificación de Contratos

### Listar topics

```bash
docker exec -it master kafka-topics.sh --bootstrap-server master:9092 --list
```

### Describir topic

```bash
docker exec -it master kafka-topics.sh --bootstrap-server master:9092 --topic datos_crudos --describe
```

### Verificar retention

```bash
docker exec -it master kafka-configs.sh --bootstrap-server master:9092 --/entity-type topics --entity-name datos_crudos --describe
```

### Consumir eventos de prueba

```bash
docker exec -it master kafka-console-consumer.sh --bootstrap-server master:9092 --topic datos_crudos --from-beginning --max-messages 5
```