# AGENTS.md
Guidance for coding agents working in this repository.

## Project Snapshot
- Stack: Kafka, Spark, Cassandra, Hive/HDFS, Airflow, Zeppelin, NiFi, Docker Compose.
- Main languages: Python, Bash, YAML.
- Main code areas: `ingesta/`, `jobs/spark/`, `jobs/cassandra/`, `airflow/dags/`, `start.sh`, `stop.sh`.

## Other Agent Rules
- Existing `AGENTS.md`: none existed before this file.
- `.cursor/rules/`: not present.
- `.cursorrules`: not present.
- `.github/copilot-instructions.md`: not present.
- Follow conventions inferred from checked-in code.
- Keep `SESSION_STATUS.md` updated when a relevant work block ends so the next session can resume quickly.

## Environment And Dependencies
- The repo is script-driven; there is no single app package.
- Some docs assume `/home/hadoop/PROYECTOLOGISTICA`; adapt commands to the current checkout.
- Python dependencies are split across `ingesta/requirements.txt`, `jobs/requirements.txt`, and `airflow/requirements.txt`.

## Build And Run Commands
### Docker and environment
- Full stack: `./start.sh` then choose `1`.
- Simple stack: `./start.sh` then choose `2`.
- Stop stack: `./stop.sh`.
- Direct full Compose start: `docker-compose up -d`.
- Direct simple Compose start: `docker-compose -f docker-compose.simple.yml up -d`.
- Build base image: `docker build -t logistica .`.

### Python environments
- Ingesta:
  - `python3 -m venv .venv-ingesta`
  - `source .venv-ingesta/bin/activate`
  - `pip install --upgrade pip && pip install -r ingesta/requirements.txt`
- Jobs:
  - `python3 -m venv .venv-jobs`
  - `source .venv-jobs/bin/activate`
  - `pip install --upgrade pip && pip install -r jobs/requirements.txt`
- Airflow:
  - `python3 -m venv .venv`
  - `source .venv/bin/activate`
  - `pip install --upgrade pip && pip install -r airflow/requirements.txt`

### Common script commands
- GPS producer: `python3 ingesta/productores/ships_gps_producer.py --bootstrap master:9092 --topic datos_crudos`
- Alerts producer: `python3 ingesta/productores/alerts_producer.py --bootstrap master:9092 --topic alertas_globales`
- Raw sink: `python3 ingesta/consumidores/kafka_to_hdfs_raw.py --bootstrap master:9092 --group-id logistica-raw-sink --spool-dir /tmp/logistica_spool --flush-every-sec 30`
- Spark raw-to-staging: `spark-submit jobs/spark/01_raw_to_staging.py`
- Spark master dimensions: `spark-submit jobs/spark/01_load_master_dimensions.py`
- Spark weather filtered-to-staging: `spark-submit jobs/spark/01_weather_filtered_to_staging.py --bootstrap kafka:9092 --topic datos_filtrados`
- Spark weather-port enrichment: `spark-submit jobs/spark/02_weather_port_enrichment.py`
- Spark weather operational fact: `spark-submit jobs/spark/03_weather_operational_fact.py`
- Spark latest vehicle state to Cassandra: `bash scripts/65_load_vehicle_latest_state_cassandra.sh`
- Spark/Hive demo rebuild after restart: `bash scripts/66_rebuild_hive_demo_tables.sh`
- Spark graph metrics: `spark-submit --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 jobs/spark/02_graph_metrics.py`
- Spark score and alert: `spark-submit jobs/spark/03_score_and_alert.py`
- Airflow webserver: `AIRFLOW_HOME=$PWD/.airflow airflow webserver --port 8080`
- Airflow scheduler: `AIRFLOW_HOME=$PWD/.airflow airflow scheduler`

## Lint, Format, And Test Reality
- No committed linter config was found.
- No committed formatter config was found.
- No committed type-checker config was found.
- No `tests/` directory or `test_*.py` files were found.
- There is no formal automated test suite today.
- Validate with the lightest real command that matches the touched area.

## Validation Commands
### Syntax and imports
- One file: `python3 -m py_compile path/to/file.py`
- Many files: `python3 -m compileall ingesta jobs airflow`

### CLI smoke checks
- Producer help: `python3 ingesta/productores/ships_gps_producer.py --help`
- Raw sink help: `python3 ingesta/consumidores/kafka_to_hdfs_raw.py --help`
- Airflow DAG syntax: `python3 -m py_compile airflow/dags/logistica_kdd_dag.py`

### Compose validation
- Full config: `docker-compose config`
- Simple config: `docker-compose -f docker-compose.simple.yml config`

### Spark validation
- Run the smallest affected Spark job directly with `spark-submit`.
- Example single-job checks:
  - `spark-submit jobs/spark/01_raw_to_staging.py`
  - `spark-submit jobs/spark/01_load_master_dimensions.py`
  - `spark-submit jobs/spark/01_weather_filtered_to_staging.py --bootstrap kafka:9092 --topic datos_filtrados`
  - `spark-submit jobs/spark/02_weather_port_enrichment.py`
  - `spark-submit jobs/spark/03_weather_operational_fact.py`
  - `spark-submit --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 jobs/spark/02_graph_metrics.py`

## Single-Test Guidance
- There is no committed test runner for true single-test execution today.
- If you add pytest tests, use `pytest tests/test_file.py::test_name -q`.
- If you add unittest tests, use `python3 -m unittest path.to.module.TestClass.test_method`.
- Until then, the smallest useful validation unit is one file, one script, or one Spark job.

## Code Style Guidelines
### General
- Match existing repository style before introducing new patterns.
- Keep code procedural and direct; this repo is script-heavy.
- Preserve existing Spanish logistics terminology such as `alertas`, `riesgo`, `clima`, `noticias`, and `logistica`.
- Prefer small helpers for reusable timestamp, schema, or transformation logic.
- Avoid introducing frameworks or abstractions that are not already used.

### Imports
- In Python files that already use it, keep `from __future__ import annotations` at the top.
- Group imports as standard library, third-party, then local.
- Separate groups with one blank line.
- Prefer explicit imports from `pyspark.sql.functions` and `pyspark.sql.types`.
- Use aliases only when they remove name conflicts or improve readability.

### Formatting
- Use 4-space indentation in Python.
- Keep lines readable; there is no enforced formatter, so optimize for clarity.
- Use parentheses for multi-line Spark chains and long calls.
- Format schemas, dicts, and chained transformations vertically when they span multiple lines.
- Keep comments brief and focused on pipeline intent, not obvious syntax.

### Types
- Add return annotations for helpers and `main()` when editing Python.
- Use builtin generics like `list[str]` and `dict[str, Path]` where helpful.
- Prefer explicit `StructType` schemas for raw Spark inputs over inference.
- Do not assume strict static typing unless the repo later adopts it.

### Naming
- Use `snake_case` for functions, variables, modules, and CLI flags.
- Use `UPPER_SNAKE_CASE` for constants and shared schemas.
- Keep numbered Spark filenames only for ordered pipeline stages like `01_*`, `02_*`, `03_*`.
- Prefer descriptive names such as `risk_by_route`, `alerts_final`, or `current_minute_hdfs`.

### Error handling
- Prefer fail-fast behavior over silent recovery.
- For subprocesses, use `check=True` unless failure is explicitly tolerated.
- Catch exceptions only when adding useful context or fallback behavior.
- Avoid broad `except Exception:` around whole jobs.
- For bad data, prefer explicit filtering, defaults, or `dropna`/`dropDuplicates` patterns.

### CLI and script structure
- New runnable Python files should expose `main()` and end with `if __name__ == "__main__": main()`.
- Use `argparse` for new script arguments.
- Keep bootstrap servers, topics, paths, and intervals configurable by flags or env vars.
- Print short status messages that help manual execution.

### Spark conventions
- Build `SparkSession` inside `main()` with chained builder calls.
- Enable Hive support only in jobs that actually need Hive tables.
- Name intermediate DataFrames clearly rather than nesting large expressions.
- Keep explicit output paths when writing curated datasets.
- Be careful with `DROP TABLE IF EXISTS`; it is acceptable here only for reproducible overwrite-style jobs.

### Airflow, shell, and Docker
- Keep DAG definitions declarative and task IDs explicit.
- Preserve `>>` task ordering style in Airflow DAGs.
- Keep shell scripts simple Bash with `set -e` where appropriate.
- Quote shell variables unless unquoted expansion is intentional.
- Preserve Compose service names, topic names, SMTP defaults, and HDFS paths unless the task requires changing them.

## Change Guidance For Agents
- Make minimal, scoped edits.
- Do not globally reformat code just because no formatter is enforced.
- Keep the repo usable in both Docker-based and local-script workflows.
- When a meaningful milestone is completed, validated, and confirmed to work correctly, create a git commit on the current branch with a concise message that reflects the milestone.
- If you add tests or tooling, update this file with the exact command to run them, including a single-test example.
- When you finish a meaningful analysis or implementation block, update `SESSION_STATUS.md` with current phase status, completed work, pending work, and the next recommended step.
