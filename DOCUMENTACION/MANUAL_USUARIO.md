# Manual de Usuario

Autora del proyecto: **Ana Coloma Bausela**

## 1. Qué hace la aplicación

El proyecto permite supervisar una cadena logística marítima y terrestre desde Asia hasta España y Francia.

El usuario puede:

- ver el estado del stock en Valladolid
- ver pedidos de clientes en Douai y Cleon
- seguir barcos con GPS y ETA
- revisar alertas logísticas
- evaluar contingencias por envío aéreo
- consultar un Gantt por semanas industriales

## 2. Cómo arrancar el proyecto

Desde la carpeta raíz:

```bash
docker-compose up -d postgres kafka nifi spark cassandra namenode datanode airflow-webserver
```

Después, para abrir el dashboard:

```bash
bash scripts/67_run_dashboard.sh
```

## 3. Qué verá el usuario

El dashboard está organizado por secciones:

- Resumen Ejecutivo
- Control Tower Valladolid
- Arquitectura en vivo
- KDD Fase I - Ingesta
- KDD Fase II - Spark
- GraphFrames
- Persistencia
- Orquestación
- Evidencias KDD

## 4. Uso recomendado

### Paso 1. Revisar servicios

En la barra lateral se puede comprobar si están activos:

- Kafka
- NiFi
- Spark
- Cassandra
- NameNode
- DataNode
- Airflow

### Paso 2. Ir a Control Tower Valladolid

Aquí se muestra:

- stock por artículo
- pedidos del cliente
- ETA de barcos
- Gantt logístico
- contingencia aérea

### Paso 3. Filtrar por cliente

En la barra lateral se puede seleccionar:

- `Douai`
- `Cleon`
- `Todos`

El contenido cambia según el cliente elegido.

### Paso 4. Filtrar por semana industrial

La vista permite escoger una semana concreta para revisar:

- pedidos
- stock
- cobertura
- planificación

### Paso 5. Simular incidencias por barco

En la pestaña de ingesta se puede seleccionar un barco y activar incidencias como:

- huelga
- tormenta
- problemas geopolíticos
- piratas
- problemas técnicos
- huelga portuaria

El sistema recalcula:

- ETA
- cobertura de stock
- si cubre o no cubre
- recomendación de contingencia aérea

## 5. Colores y significado

### Stock

- Verde: stock suficiente
- Naranja: stock tensionado
- Rojo: riesgo de rotura

### Servicios

- OK: servicio correcto
- NOK: problema detectado
- OFF: servicio parado

## 6. Interpretación de la contingencia aérea

Cuando un envío marítimo no cubre el stock, el sistema propone una alternativa aérea.

La tabla de contingencia muestra:

- referencia
- pedido afectado
- alternativa aérea
- llegada a Valladolid
- sobrecoste

## 7. Qué hacer si algo no aparece

Si el dashboard no refleja los últimos cambios:

```bash
docker-compose exec -T spark spark-submit /home/jovyan/jobs/spark/01_load_master_dimensions.py
docker-compose exec -T spark spark-submit /home/jovyan/jobs/spark/99_dashboard_bundle.py
```

Y después refrescar el navegador.
