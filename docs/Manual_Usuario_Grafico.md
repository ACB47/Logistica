# Manual de Usuario Gráfico

Autora del proyecto: **Ana Coloma Bausela**

## 1. Entrada al dashboard

Abrir:

- `http://localhost:8501`

La aplicación se presenta como una **Control Tower** logística.

## 2. Barra lateral

La barra lateral izquierda es el punto principal de control.

Incluye:

- identidad corporativa
- navegación por secciones mediante botones
- acciones globales
- estado del stack y enlaces técnicos
- documentación enlazada desde el propio sidebar

## 3. Pantallas principales

### 3.1 Resumen Ejecutivo

Muestra una vista rápida del sistema:

- flota monitorizada
- alertas críticas
- referencias en riesgo
- lead time medio
- resumen ROI de contingencia aérea

### 3.2 Control Tower Valladolid

Es la pantalla principal de negocio.

Elementos visibles:

- tabla de stock por artículo
- gráfico de barras del stock por referencia
- pedidos del cliente
- tabla de barcos con ETA
- Gantt por semanas industriales
- Gantt de cobertura de stock vs ETA marítima
- tabla de contingencia aérea
- seguimiento de 10 barcos con nombres reales

### 3.3 Arquitectura en vivo

Muestra el estado operativo de servicios Docker:

- botones `ON`
- botones `OFF`
- estado de cada servicio
- cabecera técnica con enlaces rápidos a consolas web del stack

### 3.4 Ingesta

En esta sección se ve:

- tabla de topics Kafka
- tabla de barcos con ETA y origen/destino
- mapa GPS de barcos sobre corredores marítimos
- alerta visual si un barco deja de cubrir stock

## 4. Filtros principales

### Barco concreto

Permite seguir un barco específico en la pestaña de ingesta.

## 5. Simulación visual de incidencias

Al seleccionar un barco, se pueden activar incidencias con botones tipo toggle:

- Huelga
- Tormenta
- Problemas geopolíticos
- Piratas
- Problemas técnicos del barco
- Huelga de operaciones portuarias

Al activarlas, el dashboard actualiza visualmente:

- ETA recalculada
- semáforo de cobertura
- alerta de envío aéreo si no cubre

## 6. Qué debe mirar el evaluador

Durante la exposición es recomendable enseñar en este orden:

1. estado del stack
2. Control Tower Valladolid
3. Ingesta y GPS de barcos
4. contingencia aérea
5. GraphFrames
6. Persistencia
7. Airflow

## 7. Problemas frecuentes

### No aparecen cambios en pantalla

Regenerar datos:

```bash
docker compose exec -T spark spark-submit /home/jovyan/jobs/spark/99_dashboard_bundle.py
```

Y refrescar navegador.

### No aparecen tablas nuevas

Recargar dimensiones:

```bash
docker compose exec -T spark spark-submit /home/jovyan/jobs/spark/01_load_master_dimensions.py
```

### Servicios caídos

Volver a levantar stack:

```bash
docker compose up -d postgres kafka nifi spark cassandra namenode datanode airflow-webserver
```

### No arranca el flujo de NiFi

Si NiFi se levanta desde el dashboard, el flujo Open-Meteo debería arrancar automáticamente. Si no ocurre, verificar:

```bash
bash scripts/61_nifi_healthcheck.sh
python3 scripts/62_bootstrap_nifi_open_meteo_flow.py
```
