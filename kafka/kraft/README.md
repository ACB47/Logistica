# Kafka en modo KRaft (3 nodos)

Objetivo: desplegar Kafka **sin ZooKeeper** en 3 VMs:

- `master`: broker+controller
- `slave01`: broker+controller
- `slave02`: broker+controller

> Con 3 nodos puedes tener quorum de controllers (recomendado).

## 1) Prerrequisitos

- Kafka ya disponible en la VM. En este proyecto se usará:
  - `KAFKA_HOME=/home/hadoop/Descargas/kafka_2.13-4.1.1`
- (Opcional) Instalación alternativa: `scripts/10_install_kafka_kraft.sh`.
- Hostnames resolviendo entre VMs (`/etc/hosts`).
- Puertos abiertos entre VMs:
  - 9092 (broker)
  - 9093 (controller)

## 2) Configuración por VM

En cada VM crea su fichero `server.properties` a partir de `kafka/kraft/server.properties.template`
y ajusta:

- `node.id`
- `listeners` / `advertised.listeners` (hostname correcto)
- `controller.quorum.voters` (los 3 nodos)
- `log.dirs` (disco local)

## 3) Formatear el storage KRaft (una vez por VM)

Primero genera un `cluster.id` (solo una vez, copia el mismo a las 3 VMs):

```bash
KAFKA_HOME=/home/hadoop/Descargas/kafka_2.13-4.1.1
$KAFKA_HOME/bin/kafka-storage.sh random-uuid
```

Luego, en **cada VM**, formatea:

```bash
$KAFKA_HOME/bin/kafka-storage.sh format -t <CLUSTER_ID> -c /ruta/a/server.properties
```

## 4) Arrancar Kafka (cada VM)

```bash
$KAFKA_HOME/bin/kafka-server-start.sh /ruta/a/server.properties
```

## 5) Verificación

Desde `master`:

```bash
kafka-topics.sh --bootstrap-server master:9092 --list
```

