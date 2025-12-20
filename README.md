# Debezium-Postgre-Kafka

![Python](https://img.shields.io/badge/Python-3.8%2B-blue) ![Docker Compose](https://img.shields.io/badge/Docker%20Compose-compatible-blue?logo=docker) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql) ![Kafka](https://img.shields.io/badge/Kafka-7.4.1-orange?logo=apachekafka) ![Debezium](https://img.shields.io/badge/Debezium-1.9-blue?logo=debezium)

This repository contains examples and tooling for experimenting with Change Data Capture (CDC) from PostgreSQL using Debezium and Kafka, plus a small local faker tool to generate test data for the `finance` database.

Reference article: "True Streaming: Building Change Data Capture with Debezium" by Rajat Roy
- Link: https://medium.com/@uk.rajatroy/true-streaming-building-change-data-capture-with-debezium-9466f30021e7

Quick links:
- Docker setups: see `postgre-debezium-kafka` and `postgre-debezium-S3` folders for docker-compose and connector JSON examples.
- Faker test-data tool: `faker-data/` — generates inserts/updates/deletes against `finance` DB to exercise Debezium.

How the faker tool relates to the article:
- The Medium article explains how Debezium reads PostgreSQL WAL and emits change events (create/update/delete). The `faker-data` tool generates DML activity (inserts/updates/deletes) so you can observe Debezium producing CDC events into Kafka topics.

Run (quick examples)
--------------------

1) Create and activate a virtualenv, install requirements:

```powershell
cd "E:\01. Python Basic\postgres-debezium\Debezium-Postgre-Kafka\faker-data"
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

2) Non-interactive run (example):

```powershell
python faker_db_populate.py --host localhost --port 5432 --user pguser --password 'pgpassword' --dbname finance --operations 200 --tables fund_metadata,fund_unit
```

3) Interactive helper (prompts for credentials):

```powershell
.\run_faker.ps1
```

4) Use full libpq connection string:

```powershell
python faker_db_populate.py --conn "host=localhost port=5432 user=pguser password=pgpassword dbname=finance" --operations 200 --tables fund_metadata,fund_unit
```

5) Verify counts in the DB:

```powershell
docker exec -i pg_local psql -U pguser -d finance -c "SELECT count(*) FROM public.fund_metadata; SELECT count(*) FROM public.fund_unit;"
```

Tips
----
- To only target `fund_metadata` and `fund_unit`: use `--tables fund_metadata,fund_unit`.
- `--operations N` executes N total random actions (insert/update/delete); defaults to 200.
