# Faker DB populater

This script populates the `finance` database tables with randomized inserts, updates and deletes.

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
