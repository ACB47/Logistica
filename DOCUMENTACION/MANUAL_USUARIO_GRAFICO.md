# Manual de Usuario Gráfico

Autora del proyecto: **Ana Coloma Bausela**

## 1. Entrada al dashboard

Abrir:

- `http://localhost:8501`

La aplicación se presenta como una **Control Tower** logística.

## 2. Barra lateral

La barra lateral izquierda es el punto principal de control.

Incluye:

- estado del stack
- accesos rápidos
- acciones globales
- navegación por secciones
- parámetros de demo
- filtros de cliente y semana industrial
- incidencias por barco

## 3. Pantallas principales

### 3.1 Resumen Ejecutivo

Muestra una vista rápida del sistema:

- flota monitorizada
- cobertura media de stock
- ETA de barcos
- alertas
- riesgo operativo

### 3.2 Control Tower Valladolid

Es la pantalla principal de negocio.

Elementos visibles:

- tabla de stock por artículo
- gráfico de barras del stock por referencia
- pedidos del cliente seleccionado
- tabla de barcos con ETA
- Gantt por semanas industriales
- Gantt de cobertura de stock vs ETA marítima
- tabla de contingencia aérea

### 3.3 Arquitectura en vivo

Muestra el estado operativo de servicios Docker:

- botón `On`
- botón `Off`
- estado de cada servicio

### 3.4 Ingesta

En esta sección se ve:

- tabla de topics Kafka
- tabla de barcos con ETA y origen/destino
- mapa GPS de barcos
- alerta visual si un barco deja de cubrir stock

## 4. Filtros principales

### Cliente destino

Permite cambiar entre:

- `Douai`
- `Cleon`
- `Todos`

### Semana industrial

Permite cambiar el horizonte de análisis.

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
docker-compose exec -T spark spark-submit /home/jovyan/jobs/spark/99_dashboard_bundle.py
```

Y refrescar navegador.

### No aparecen tablas nuevas

Recargar dimensiones:

```bash
docker-compose exec -T spark spark-submit /home/jovyan/jobs/spark/01_load_master_dimensions.py
```

### Servicios caídos

Volver a levantar stack:

```bash
docker-compose up -d postgres kafka nifi spark cassandra namenode datanode airflow-webserver
```
