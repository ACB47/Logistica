# Origen de las Tablas Dimensión

Este documento especifica el origen y criterio de join de cada dimensión del proyecto.

---

## dim_ports

**Origen:** Datos maestros embebidos en `jobs/spark/01_load_master_dimensions.py`
**Descripción:** Catálogo de puertos marítimos del proyecto.
**Criterio de join:** `port_code` -> `origin_port` / `dest_port`

| Campo | Tipo | Descripción | Origen |
|-------|------|------------|--------|
| `port_code` | string | Código único del puerto | Definido en código |
| `port_name` | string | Nombre descriptivo | Definido en código |
| `country` | string | Código país ISO | Definido en código |
| `region` | string | Región geográfica | Definido en código |
| `lat` | float | Latitud | Definido en código |
| `lon` | float | Longitud | Definido en código |
| `port_type` | string | Tipo: `port`, `origin_hub` | Definido en código |

---

## dim_routes

**Origen:** Datos maestros embebidos en `jobs/spark/01_load_master_dimensions.py`
**Descripción:** Rutas marítimas entre puertos.
**Criterio de join:** `route_id` -> `route_id`

| Campo | Tipo | Descripción | Origen |
|-------|------|------------|--------|
| `route_id` | string | ID único de ruta | Definido en código |
| `origin_port` | string | Puerto de origen | Definido en código |
| `dest_port` | string | Puerto de destino | Definido en código |
| `mode` | string |Modo de transporte | Definido en código |
| `transit_hours` | float | Horas de tránsito | Estimación |
| `transit_days` | float | Días de tránsito | Estimación |
| `cost_eur` | float | Coste en euros | Estimación |

---

## dim_warehouse

**Origen:** Datos maestros embebidos en `jobs/spark/01_load_master_dimensions.py`
**Descripción:** Catálogo de almacenes.
**Criterio de join:** `warehouse_code` -> `warehouse`

| Campo | Tipo | Descripción | Origen |
|-------|------|------------|--------|
| `warehouse_code` | string | Código único | Definido en código |
| `warehouse_name` | string | Nombre | Definido en código |
| `country` | string | País | Definido en código |
| `region` | string | Región | Definido en código |
| `warehouse_type` | string | Tipo: `cross_dock` | Definido en código |
| `lat` | float | Latitud | Definido en código |
| `lon` | float | Longitud | Definido en código |

---

## dim_articles_valladolid

**Origen:** Datos embebidos en `jobs/spark/01_load_master_dimensions.py`
**Descripción:** Catálogo de artículos con stock en Valladolid.
**Criterio de join:** `article_ref` -> `article_ref`

| Campo | Tipo | Descripción | Origen |
|-------|------|------------|--------|
| `article_ref` | string | Referencia del artículo | Definido en código |
| `article_name` | string | Descripción | Definido en código |
| `category` | string | Categoría | Definido en código |
| `total_stock_pieces` | int | Stock total | Definido en código |
| `safety_stock_min` | int | Stock mínimo seguridad | Definido en código |
| `daily_consumption_avg` | float | Consumo diario promedio | Definido en código |
| `weekly_consumption_avg` | float | Consumo semanal | Definido en código |
| `reorder_qty` | int | Cantidad de reposición | Definido en código |
| `warehouse` | string | Almacén | Definido en código |
| `inbound_port` | string | Puerto de entrada | Definido en código |

---

## fact_customer_orders_douai

**Origen:** Datos embebidos en `jobs/spark/01_load_master_dimensions.py`
**Descripción:** Pedidos de clientes Francia (Douai, Cléon).
**Criterio de join:** `order_id`, `article_ref` -> `article_ref`

| Campo | Tipo | Descripción | Origen |
|-------|------|------------|--------|
| `order_id` | string | ID de pedido | Definido en código |
| `article_ref` | string | Referencia artículo | Definido en código |
| `customer_name` | string | Cliente | Definido en código |
| `customer_country` | string | País | Definido en código |
| `industrial_week` | string | Semana industrial | Definido en código |
| `requested_date` | string | Fecha solicitada | Definido en código |
| `ship_date` | string | Fecha de envío | Definido en código |
| `quantity_pieces` | int | Cantidad piezas | Definido en código |
| `total_value_eur` | float | Valor total euros | Definido en código |
| `order_status` | string | Estado | Definido en código |

---

## fact_article_gantt

**Origen:** Datos embebidos en `jobs/spark/01_load_master_dimensions.py`
**Descripción:** Gantt de planificación por artículo y semana industrial.
**Criterio de join:** `article_ref` -> `article_ref`

| Campo | Tipo | Descripción | Origen |
|-------|------|------------|--------|
| `article_ref` | string | Referencia artículo | Definido en código |
| `task_name` | string | Nombre de tarea | Definido en código |
| `start_date` | string | Fecha inicio | Definido en código |
| `end_date` | string | Fecha fin | Definido en código |
| `industrial_week` | string | Semana industrial | Definido en código |
| `task_type` | string | Tipo: `sea`, `air`, `truck`, `warehouse` | Definido en código |

---

## join con Staging

```python
# Ejemplo de join con staging
ships_stg = spark.table("logistica.stg_ships")
ports = spark.table("logistica.dim_ports")

joined = ships_stg.join(ports, ships_stg.origin_port == ports.port_code)
```

---

## Verificación

```bash
# Listar todas las dimensiones
docker exec -it spark /opt/spark/bin/beeline -u "jdbc:hive2://hiveserver:10000" -e "SHOW TABLES IN logistica;"

# Verificar conteos
docker exec -it spark /opt/spark/bin/beeline -u "jdbc:hive2://hiveserver:10000" -e "SELECT 'dim_ports' as tbl, COUNT(*) as cnt FROM logistica.dim_ports;"
```