
![Python](https://img.shields.io/badge/Python-3.8%2B-blue) ![Docker Compose](https://img.shields.io/badge/Docker%20Compose-compatible-blue?logo=docker) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql) ![Debezium](https://img.shields.io/badge/Debezium-1.9-blue?logo=debezium)

# Faker DB populater

This script populates the `finance` database tables with randomized inserts, updates and deletes.

Related article: "True Streaming: Building Change Data Capture with Debezium" by Rajat Roy
 - https://medium.com/@uk.rajatroy/true-streaming-building-change-data-capture-with-debezium-9466f30021e7

Prerequisites

- Python 3.8+
- PostgreSQL running and reachable (local by default)
- Install requirements:

```bash
python -m pip install -r requirements.txt
```

Usage

1) Create and activate virtualenv, install deps:

```powershell
cd "E:\01. Python Basic\postgres-debezium\Debezium-Postgre-Kafka\faker-data"
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

2) Non-interactive run (example):

```powershell
python faker_db_populate.py --host localhost --port 5432 --user pguser --password secret --dbname finance --operations 300 --tables fund_metadata,fund_unit
```

3) Interactive helper (prompts for credentials):

```powershell
.\run_faker.ps1
```

4) Or run with a connection string:

```powershell
python faker_db_populate.py --conn "host=localhost port=5432 user=pguser password=secret dbname=finance" --operations 250
```

Notes

- The script introspects the `public` schema for these tables: `awsdms_ddl_audit`, `fund_metadata`, `fund_unit`, `party`, `policy` and will only act on tables that exist.
- It attempts to infer column types and generate sensible fake values.
- Adjust `--operations` to control total number of insert/update/delete actions (default 200).

How this helps with Debezium testing
 - The Medium article shows how Debezium captures WAL changes and emits change events to Kafka topics. Use this faker tool to create realistic DML load so you can observe Debezium events for `fund_metadata` and `fund_unit` topics (or the tables you enable in your connector `table.include.list`).

How this helps with Debezium testing
 - The Medium article shows how Debezium captures WAL changes and emits change events to Kafka topics. Use this faker tool to create realistic DML load so you can observe Debezium events for `fund_metadata` and `fund_unit` topics (or the tables you enable in your connector `table.include.list`).

