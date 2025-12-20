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

```bash
python faker_db_populate.py --host localhost --port 5432 --user pguser --password secret --dbname finance --operations 300
```

Or with a connection string:

```bash
python faker_db_populate.py --conn "host=localhost port=5432 user=pguser password=secret dbname=finance" --operations 250
```

Notes

- The script introspects the `public` schema for these tables: `awsdms_ddl_audit`, `fund_metadata`, `fund_unit`, `party`, `policy` and will only act on tables that exist.
- It attempts to infer column types and generate sensible fake values.
- Adjust `--operations` to control total number of insert/update/delete actions (default 200).

How this helps with Debezium testing
 - The Medium article shows how Debezium captures WAL changes and emits change events to Kafka topics. Use this faker tool to create realistic DML load so you can observe Debezium events for `fund_metadata` and `fund_unit` topics (or the tables you enable in your connector `table.include.list`).

