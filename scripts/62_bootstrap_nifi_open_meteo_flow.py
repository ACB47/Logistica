#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import ssl
import sys
import urllib.parse
import urllib.request


BASE_URL = os.getenv("NIFI_BASE_URL", "https://localhost:8443/nifi-api")
USERNAME = os.getenv("NIFI_USERNAME", "admin")
PASSWORD = os.getenv("NIFI_PASSWORD", "CHANGE_ME_NIFI_PASSWORD")
OPEN_METEO_LATITUDE = os.getenv("OPEN_METEO_LATITUDE", "36.1408")
OPEN_METEO_LONGITUDE = os.getenv("OPEN_METEO_LONGITUDE", "-5.3536")
OPEN_METEO_TIMEZONE = os.getenv("OPEN_METEO_TIMEZONE", "Europe/Madrid")
FLOW_NAME = os.getenv("NIFI_FLOW_NAME", "OpenMeteo Kafka Flow")
START_FLOW = os.getenv("START_NIFI_FLOW", "0") == "1"

SSL_CONTEXT = ssl._create_unverified_context()


def api_call(
    path: str,
    method: str = "GET",
    data: dict | None = None,
    headers: dict[str, str] | None = None,
    form: bool = False,
):
    request_headers = dict(headers or {})
    body = None

    if data is not None:
        if form:
            body = urllib.parse.urlencode(data).encode()
            request_headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
        else:
            body = json.dumps(data).encode()
            request_headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=body,
        headers=request_headers,
        method=method,
    )

    with urllib.request.urlopen(request, context=SSL_CONTEXT) as response:
        raw = response.read().decode()
        if response.headers.get_content_type() == "application/json":
            return json.loads(raw)
        return raw


def build_open_meteo_url() -> str:
    query = urllib.parse.urlencode(
        {
            "latitude": OPEN_METEO_LATITUDE,
            "longitude": OPEN_METEO_LONGITUDE,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,weather_code",
            "timezone": OPEN_METEO_TIMEZONE,
        }
    )
    return f"https://api.open-meteo.com/v1/forecast?{query}"


def token_headers() -> dict[str, str]:
    token = api_call(
        "/access/token",
        method="POST",
        data={"username": USERNAME, "password": PASSWORD},
        form=True,
    )
    return {"Authorization": f"Bearer {token}"}


def create_process_group(root_id: str, root_revision: int, headers: dict[str, str]) -> dict:
    body = {
        "revision": {"version": root_revision},
        "component": {
            "name": FLOW_NAME,
            "comments": "Flujo inicial Open-Meteo -> Kafka raw/filtered",
            "position": {"x": 400.0, "y": 200.0},
        },
    }
    return api_call(f"/process-groups/{root_id}/process-groups", method="POST", data=body, headers=headers)


def create_controller_service(group_id: str, headers: dict[str, str]) -> dict:
    body = {
        "revision": {"version": 0},
        "component": {
            "name": "Kafka3ConnectionService",
            "type": "org.apache.nifi.kafka.service.Kafka3ConnectionService",
            "bundle": {
                "group": "org.apache.nifi",
                "artifact": "nifi-kafka-3-service-nar",
                "version": "2.6.0",
            },
        },
    }
    return api_call(f"/process-groups/{group_id}/controller-services", method="POST", data=body, headers=headers)


def update_controller_service(service_id: str, service_revision: int, headers: dict[str, str]) -> dict:
    current = api_call(f"/controller-services/{service_id}", headers=headers)
    body = {
        "revision": {"version": service_revision},
        "component": {
            "id": service_id,
            "parentGroupId": current["component"]["parentGroupId"],
            "name": current["component"]["name"],
            "type": current["component"]["type"],
            "bundle": current["component"]["bundle"],
            "state": current["component"]["state"],
            "properties": {
                "bootstrap.servers": "kafka:9092",
                "security.protocol": "PLAINTEXT",
            },
        },
    }
    return api_call(f"/controller-services/{service_id}", method="PUT", data=body, headers=headers)


def enable_controller_service(service_id: str, revision: int, headers: dict[str, str]) -> dict:
    body = {
        "revision": {"version": revision},
        "state": "ENABLED",
    }
    return api_call(f"/controller-services/{service_id}/run-status", method="PUT", data=body, headers=headers)


def start_processor(processor_id: str, revision: int, headers: dict[str, str]) -> dict:
    body = {
        "revision": {"version": revision},
        "state": "RUNNING",
    }
    return api_call(f"/processors/{processor_id}/run-status", method="PUT", data=body, headers=headers)


def refresh_processor(processor_id: str, headers: dict[str, str]) -> dict:
    return api_call(f"/processors/{processor_id}", headers=headers)


def refresh_controller_service(service_id: str, headers: dict[str, str]) -> dict:
    return api_call(f"/controller-services/{service_id}", headers=headers)


def get_process_group_flow(group_id: str, headers: dict[str, str]) -> dict:
    return api_call(f"/flow/process-groups/{group_id}", headers=headers)


def update_processor(
    processor_id: str,
    revision: int,
    headers: dict[str, str],
    properties: dict[str, str] | None = None,
    auto_terminated: list[str] | None = None,
    scheduling_period: str | None = None,
):
    current = api_call(f"/processors/{processor_id}", headers=headers)
    config: dict[str, object] = {"properties": properties or {}}
    if auto_terminated is not None:
        config["autoTerminatedRelationships"] = auto_terminated

    body = {
        "revision": {"version": revision},
        "component": {
            "id": processor_id,
            "name": current["component"]["name"],
            "config": {
                **config,
            },
        },
    }

    if scheduling_period is not None:
        body["component"]["schedulingPeriod"] = scheduling_period

    return api_call(f"/processors/{processor_id}", method="PUT", data=body, headers=headers)


def create_processor(group_id: str, name: str, processor_type: str, bundle: dict[str, str], x: float, y: float, headers: dict[str, str]) -> dict:
    body = {
        "revision": {"version": 0},
        "component": {
            "type": processor_type,
            "bundle": bundle,
            "name": name,
            "position": {"x": x, "y": y},
        },
    }
    return api_call(f"/process-groups/{group_id}/processors", method="POST", data=body, headers=headers)


def create_connection(
    group_id: str,
    source_id: str,
    destination_id: str,
    relationship: str,
    headers: dict[str, str],
) -> dict:
    body = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": source_id, "groupId": group_id, "type": "PROCESSOR"},
            "destination": {"id": destination_id, "groupId": group_id, "type": "PROCESSOR"},
            "selectedRelationships": [relationship],
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "256 MB",
            "flowFileExpiration": "0 sec",
        },
    }
    return api_call(f"/process-groups/{group_id}/connections", method="POST", data=body, headers=headers)


def main() -> int:
    headers = token_headers()
    root = api_call("/flow/process-groups/root", headers=headers)
    root_id = root["processGroupFlow"]["id"]
    root_revision = root["revision"]["version"]

    existing = [
        pg for pg in root["processGroupFlow"]["flow"]["processGroups"] if pg["component"]["name"] == FLOW_NAME
    ]
    if existing:
        group_id = existing[0]["component"]["id"]
        print(f"El flujo '{FLOW_NAME}' ya existe en NiFi: {group_id}")
        if not START_FLOW:
            print("El flujo ya existe. No se recrea.")
            return 0

        group_flow = get_process_group_flow(group_id, headers)
        processors = {
            p["component"]["name"]: p
            for p in group_flow["processGroupFlow"]["flow"].get("processors", [])
        }
        controller_services = {
            s["component"]["name"]: s
            for s in group_flow["processGroupFlow"]["flow"].get("controllerServices", [])
        }

        service = controller_services.get("Kafka3ConnectionService")
        if service:
            refreshed_service = refresh_controller_service(service["component"]["id"], headers)
            if refreshed_service["component"].get("state") != "ENABLED":
                enable_controller_service(
                    refreshed_service["component"]["id"],
                    refreshed_service["revision"]["version"],
                    headers,
                )

        for name in [
            "PublishKafka Raw",
            "PublishKafka Filtered",
            "JoltTransformJSON Filtered",
            "RouteOnAttribute Raw",
            "RouteOnAttribute Filtered",
            "InvokeHTTP Raw",
            "InvokeHTTP Filtered",
            "GenerateFlowFile Raw",
            "GenerateFlowFile Filtered",
        ]:
            processor = processors.get(name)
            if processor is None:
                continue
            refreshed = refresh_processor(processor["component"]["id"], headers)
            if refreshed["component"].get("state") != "RUNNING":
                start_processor(refreshed["component"]["id"], refreshed["revision"]["version"], headers)
        print("Procesadores existentes arrancados.")
        print("URL NiFi: https://localhost:8443")
        return 0

    process_group = create_process_group(root_id, root_revision, headers)
    group_id = process_group["component"]["id"]
    print(f"Process group creado: {FLOW_NAME} ({group_id})")

    service = create_controller_service(group_id, headers)
    service = update_controller_service(service["component"]["id"], service["revision"]["version"], headers)
    service = enable_controller_service(service["component"]["id"], service["revision"]["version"], headers)
    service_id = service["component"]["id"]
    print(f"Controller service creado: {service_id}")

    standard_bundle = {"group": "org.apache.nifi", "artifact": "nifi-standard-nar", "version": "2.6.0"}
    jolt_bundle = {"group": "org.apache.nifi", "artifact": "nifi-jolt-nar", "version": "2.6.0"}
    kafka_bundle = {"group": "org.apache.nifi", "artifact": "nifi-kafka-nar", "version": "2.6.0"}

    processors = {}
    definitions = [
        ("GenerateFlowFile Raw", "org.apache.nifi.processors.standard.GenerateFlowFile", standard_bundle, 80.0, 80.0),
        ("InvokeHTTP Raw", "org.apache.nifi.processors.standard.InvokeHTTP", standard_bundle, 340.0, 80.0),
        ("RouteOnAttribute Raw", "org.apache.nifi.processors.standard.RouteOnAttribute", standard_bundle, 620.0, 80.0),
        ("PublishKafka Raw", "org.apache.nifi.kafka.processors.PublishKafka", kafka_bundle, 900.0, 80.0),
        ("GenerateFlowFile Filtered", "org.apache.nifi.processors.standard.GenerateFlowFile", standard_bundle, 80.0, 340.0),
        ("InvokeHTTP Filtered", "org.apache.nifi.processors.standard.InvokeHTTP", standard_bundle, 340.0, 340.0),
        ("RouteOnAttribute Filtered", "org.apache.nifi.processors.standard.RouteOnAttribute", standard_bundle, 620.0, 340.0),
        ("JoltTransformJSON Filtered", "org.apache.nifi.processors.jolt.JoltTransformJSON", jolt_bundle, 900.0, 340.0),
        ("PublishKafka Filtered", "org.apache.nifi.kafka.processors.PublishKafka", kafka_bundle, 1180.0, 340.0),
    ]

    for name, processor_type, bundle, x, y in definitions:
        processors[name] = create_processor(group_id, name, processor_type, bundle, x, y, headers)

    open_meteo_url = build_open_meteo_url()
    jolt_spec = json.dumps(
        [
            {
                "operation": "shift",
                "spec": {
                    "current": {
                        "time": "ts",
                        "temperature_2m": "temperature_c",
                        "relative_humidity_2m": "humidity_pct",
                        "wind_speed_10m": "wind_speed_kmh",
                        "wind_direction_10m": "wind_direction_deg",
                        "weather_code": "weather_code",
                    }
                },
            },
            {
                "operation": "default",
                "spec": {
                    "event_type": "weather_snapshot",
                    "source": "open_meteo",
                    "region": "Estrecho de Gibraltar",
                    "port_ref": "Algeciras",
                },
            },
        ]
    )

    update_processor(
        processors["GenerateFlowFile Raw"]["component"]["id"],
        processors["GenerateFlowFile Raw"]["revision"]["version"],
        headers,
        properties={"Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "true"},
        scheduling_period="5 min",
    )
    update_processor(
        processors["InvokeHTTP Raw"]["component"]["id"],
        processors["InvokeHTTP Raw"]["revision"]["version"],
        headers,
        properties={"HTTP Method": "GET", "HTTP URL": open_meteo_url},
        auto_terminated=["Original", "Failure", "No Retry", "Retry"],
    )
    update_processor(
        processors["RouteOnAttribute Raw"]["component"]["id"],
        processors["RouteOnAttribute Raw"]["revision"]["version"],
        headers,
        properties={"ok_api": "${invokehttp.status.code:equals('200')}"},
        auto_terminated=["unmatched"],
    )
    update_processor(
        processors["PublishKafka Raw"]["component"]["id"],
        processors["PublishKafka Raw"]["revision"]["version"],
        headers,
        properties={
            "Kafka Connection Service": service_id,
            "Topic Name": "datos_crudos",
            "Failure Strategy": "Route to Failure",
        },
        auto_terminated=["success", "failure"],
    )

    update_processor(
        processors["GenerateFlowFile Filtered"]["component"]["id"],
        processors["GenerateFlowFile Filtered"]["revision"]["version"],
        headers,
        properties={"Batch Size": "1", "Data Format": "Text", "Unique FlowFiles": "true"},
        scheduling_period="5 min",
    )
    update_processor(
        processors["InvokeHTTP Filtered"]["component"]["id"],
        processors["InvokeHTTP Filtered"]["revision"]["version"],
        headers,
        properties={"HTTP Method": "GET", "HTTP URL": open_meteo_url},
        auto_terminated=["Original", "Failure", "No Retry", "Retry"],
    )
    update_processor(
        processors["RouteOnAttribute Filtered"]["component"]["id"],
        processors["RouteOnAttribute Filtered"]["revision"]["version"],
        headers,
        properties={"ok_api": "${invokehttp.status.code:equals('200')}"},
        auto_terminated=["unmatched"],
    )
    update_processor(
        processors["JoltTransformJSON Filtered"]["component"]["id"],
        processors["JoltTransformJSON Filtered"]["revision"]["version"],
        headers,
        properties={"Jolt Transform": "jolt-transform-chain", "Jolt Specification": jolt_spec},
        auto_terminated=["failure"],
    )
    update_processor(
        processors["PublishKafka Filtered"]["component"]["id"],
        processors["PublishKafka Filtered"]["revision"]["version"],
        headers,
        properties={
            "Kafka Connection Service": service_id,
            "Topic Name": "datos_filtrados",
            "Failure Strategy": "Route to Failure",
        },
        auto_terminated=["success", "failure"],
    )

    create_connection(
        group_id,
        processors["GenerateFlowFile Raw"]["component"]["id"],
        processors["InvokeHTTP Raw"]["component"]["id"],
        "success",
        headers,
    )
    create_connection(
        group_id,
        processors["InvokeHTTP Raw"]["component"]["id"],
        processors["RouteOnAttribute Raw"]["component"]["id"],
        "Response",
        headers,
    )
    create_connection(
        group_id,
        processors["RouteOnAttribute Raw"]["component"]["id"],
        processors["PublishKafka Raw"]["component"]["id"],
        "ok_api",
        headers,
    )
    create_connection(
        group_id,
        processors["GenerateFlowFile Filtered"]["component"]["id"],
        processors["InvokeHTTP Filtered"]["component"]["id"],
        "success",
        headers,
    )
    create_connection(
        group_id,
        processors["InvokeHTTP Filtered"]["component"]["id"],
        processors["RouteOnAttribute Filtered"]["component"]["id"],
        "Response",
        headers,
    )
    create_connection(
        group_id,
        processors["RouteOnAttribute Filtered"]["component"]["id"],
        processors["JoltTransformJSON Filtered"]["component"]["id"],
        "ok_api",
        headers,
    )
    create_connection(
        group_id,
        processors["JoltTransformJSON Filtered"]["component"]["id"],
        processors["PublishKafka Filtered"]["component"]["id"],
        "success",
        headers,
    )

    service_state = refresh_controller_service(service_id, headers)["component"]["state"]
    print(f"Controller service state: {service_state}")

    if START_FLOW:
        for name in [
            "PublishKafka Raw",
            "PublishKafka Filtered",
            "JoltTransformJSON Filtered",
            "RouteOnAttribute Raw",
            "RouteOnAttribute Filtered",
            "InvokeHTTP Raw",
            "InvokeHTTP Filtered",
            "GenerateFlowFile Raw",
            "GenerateFlowFile Filtered",
        ]:
            refreshed = refresh_processor(processors[name]["component"]["id"], headers)
            start_processor(refreshed["component"]["id"], refreshed["revision"]["version"], headers)
        print("Procesadores arrancados.")

    print("Flujo creado en NiFi.")
    if not START_FLOW:
        print("Siguiente paso: arrancar los procesadores desde la UI o usando START_NIFI_FLOW=1.")
    print("URL NiFi: https://localhost:8443")
    return 0


if __name__ == "__main__":
    sys.exit(main())
