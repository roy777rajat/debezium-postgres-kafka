# Debezium-Postgre-Kafka

This repository contains examples and tooling for experimenting with Change Data Capture (CDC) from PostgreSQL using Debezium and Kafka, plus a small local faker tool to generate test data for the `finance` database.

Reference article: "True Streaming: Building Change Data Capture with Debezium" by Rajat Roy
- Link: https://medium.com/@uk.rajatroy/true-streaming-building-change-data-capture-with-debezium-9466f30021e7

Quick links:
- Docker setups: see `postgre-debezium-kafka` and `postgre-debezium-S3` folders for docker-compose and connector JSON examples.
- Faker test-data tool: `faker-data/` — generates inserts/updates/deletes against `finance` DB to exercise Debezium.

How the faker tool relates to the article:
- The Medium article explains how Debezium reads PostgreSQL WAL and emits change events (create/update/delete). The `faker-data` tool generates DML activity (inserts/updates/deletes) so you can observe Debezium producing CDC events into Kafka topics.

Run the faker tool (example):
```powershell
cd "E:\01. Python Basic\postgres-debezium\Debezium-Postgre-Kafka\faker-data"
.\venv\Scripts\Activate.ps1
python faker_db_populate.py --host localhost --port 5432 --user pguser --password 'pgpassword' --dbname finance --operations 200 --tables fund_metadata,fund_unit
```

If you want interactive credential prompts use:
```powershell
.\run_faker.ps1
```
