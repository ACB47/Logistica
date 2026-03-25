# Flujo NiFi inicial: Open-Meteo -> Kafka

Este documento define el primer flujo real de NiFi para la entrega.

## Objetivo
- Consumir una API publica real sin credenciales: Open-Meteo.
- Generar eventos de clima para el area de Algeciras/Gibraltar.
- Publicar en Kafka:
  - `datos_crudos` con la respuesta original.
  - `datos_filtrados` con un JSON simplificado listo para analitica.

## Endpoint base
- URL:
  - `https://api.open-meteo.com/v1/forecast?latitude=${OPEN_METEO_LATITUDE}&longitude=${OPEN_METEO_LONGITUDE}&current=temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,weather_code&timezone=${OPEN_METEO_TIMEZONE}`

## Credenciales locales NiFi
- Usuario: `${NIFI_USERNAME}` o `admin`
- Password: `${NIFI_PASSWORD}` o `Admin123456!`

## Topics destino
- Raw: `datos_crudos`
- Filtrado: `datos_filtrados`

## Flujo recomendado en el canvas
- `GenerateFlowFile`
  - scheduling: `5 min`
  - contenido: vacio
- `InvokeHTTP`
  - method: `GET`
  - remote url: endpoint de Open-Meteo
  - always output response: `true`
  - add response headers to request: `false`
- `RouteOnAttribute`
  - `ok_api`: `${invokehttp.status.code:equals('200')}`
  - `ko_api`: `${invokehttp.status.code:ne('200')}`
- Rama `ok_api`:
  - `DuplicateFlowFile`
  - salida A -> `PublishKafka_2_6` para `datos_crudos`
  - salida B -> `JoltTransformJSON` o `JoltTransformRecord`
  - despues -> `PublishKafka_2_6` para `datos_filtrados`
- Rama `ko_api`:
  - `LogMessage`
  - `PutFile` opcional a carpeta de errores local

## Payload filtrado recomendado
- Estructura de salida para `datos_filtrados`:

```json
{
  "event_type": "weather_snapshot",
  "ts": "2026-03-25T19:00",
  "source": "open_meteo",
  "region": "Estrecho de Gibraltar",
  "port_ref": "Algeciras",
  "temperature_c": 18.4,
  "humidity_pct": 73,
  "wind_speed_kmh": 21.3,
  "wind_direction_deg": 245,
  "weather_code": 3,
  "severity": 2
}
```

## Regla simple de severidad inicial
- `severity=1` si `wind_speed_kmh < 20`
- `severity=2` si `20 <= wind_speed_kmh < 35`
- `severity=3` si `35 <= wind_speed_kmh < 50`
- `severity=4` si `wind_speed_kmh >= 50`

## Propiedades clave de PublishKafka
- bootstrap servers: `kafka:9092`
- acknowledgement waits for all replicas: `false` en entorno local, `true` en entorno mas serio
- use transactions: `false`
- message key field:
  - raw: `open_meteo_raw`
  - filtrado: `weather_snapshot`

## Back-pressure y control de fallos
- Conexiones principales:
  - object threshold: `10000`
  - data size threshold: `256 MB`
- Retries en `InvokeHTTP`
- Cola de fallo separada para respuestas no `200`
- Si `PublishKafka` falla, rutear a `retry` y `failure`

## Evidencias a guardar
- Captura del canvas completo.
- Captura de configuracion de `InvokeHTTP`.
- Captura de configuracion de `PublishKafka_2_6`.
- Evidencia de mensajes en `datos_crudos` y `datos_filtrados`.
- Evidencia de una cola de error o ruta de fallback.

## Siguiente extension natural
- Añadir una segunda fuente publica.
- Enriquecer eventos con `UpdateRecord`.
- Mandar una copia raw adicional a HDFS o MinIO.
