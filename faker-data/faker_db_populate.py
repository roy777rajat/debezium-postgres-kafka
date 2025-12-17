#!/usr/bin/env python3
"""
faker_db_populate.py

Populate the `finance` DB tables with fake data (insert/update/delete)

Usage examples:
  python faker_db_populate.py --host localhost --port 5432 --user pguser --password secret --dbname finance --operations 300

You can also provide a full connection string using --conn
"""
import argparse
import random
import sys
from datetime import datetime, date
from uuid import uuid4

import psycopg2
from faker import Faker

fake = Faker()


def parse_args():
    p = argparse.ArgumentParser(description="Populate finance DB with fake operations")
    p.add_argument("--conn", help="Full libpq connection string (overrides host/port/user/password/dbname)")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", default=5432, type=int)
    p.add_argument("--user", required=False)
    p.add_argument("--password", required=False)
    p.add_argument("--dbname", default="finance")
    p.add_argument("--operations", type=int, default=200, help="Total number of operations to perform")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--schema", default="public")
    p.add_argument("--tables", help="Comma-separated list of tables to target (default: all target tables)")
    return p.parse_args()


def connect(args):
    if args.conn:
        return psycopg2.connect(args.conn)
    conn = psycopg2.connect(host=args.host, port=args.port, user=args.user, password=args.password, dbname=args.dbname)
    return conn


def get_columns(conn, schema, table):
    q = """
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns
    WHERE table_schema=%s AND table_name=%s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return cur.fetchall()


def get_primary_key(conn, schema, table):
    q = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND kcu.table_schema = %s
      AND kcu.table_name = %s
    ORDER BY kcu.ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        rows = cur.fetchall()
        return [r[0] for r in rows]


def gen_value(data_type, column_name=None):
    dt = data_type.lower()
    if "timestamp" in dt or "time" in dt:
        return datetime.utcnow()
    if dt in ("date",):
        return date.today()
    if dt.startswith("int") or dt in ("integer", "smallint", "bigint"):
        return random.randint(1, 9999)
    if dt in ("numeric", "decimal", "real", "double precision") or "numeric" in dt:
        return round(random.uniform(1.0, 1000.0), 2)
    if dt in ("boolean",):
        return random.choice([True, False])
    if dt in ("uuid",):
        return str(uuid4())
    # text-like
    if column_name:
        name_col = column_name.lower()
        if "name" in name_col:
            return fake.company()
        if "code" in name_col:
            return fake.lexify(text='???-????').upper()
        if "description" in name_col or "desc" in name_col:
            return fake.sentence(nb_words=6)
        if "email" in name_col:
            return fake.email()
        if "phone" in name_col or "mobile" in name_col:
            return fake.phone_number()
        if "address" in name_col or "addr" in name_col:
            return fake.address()
    return fake.word()


def insert_row(conn, schema, table, columns):
    # columns: list of (column_name, data_type, is_nullable, column_default)
    cols_to_set = []
    vals = []
    for col, dtype, is_nullable, default in columns:
        if default is not None and 'nextval' in (default or ""):
            continue
        if default is not None and default.strip().startswith('uuid_generate'):
            continue
        # Special handling for finance tables
        # For fund_metadata allow description and price to be NULL sometimes
        if table == 'fund_metadata' and col in ('fund_description', 'fund_price'):
            if random.random() < 0.25:
                # leave as NULL
                continue
        # handle simple foreign-key like columns ending with _id by picking existing value
        if col.endswith('_id'):
            base = col[:-3]
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT "{col}" FROM "{schema}"."{base}" ORDER BY random() LIMIT 1')
                    r = cur.fetchone()
                    if r:
                        cols_to_set.append(col)
                        vals.append(r[0])
                        continue
            except Exception:
                # fallback to generated value
                pass
        # If inserting into fund_unit, prefer linking fund_code to an existing fund_metadata
        if table == 'fund_unit' and col == 'fund_code':
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT fund_code FROM "{schema}"."fund_metadata" ORDER BY random() LIMIT 1')
                    r = cur.fetchone()
                    if r and random.random() < 0.8:
                        cols_to_set.append(col)
                        vals.append(r[0])
                        continue
                    else:
                        # generate a code that may or may not exist in fund_metadata
                        cols_to_set.append(col)
                        vals.append(gen_value(dtype, col))
                        continue
            except Exception:
                pass
        # prefer to supply values for non-nullable columns without defaults
        if is_nullable == 'NO' or random.random() < 0.8:
            cols_to_set.append(col)
            vals.append(gen_value(dtype, col))
    if not cols_to_set:
        return None
    placeholders = ','.join(['%s'] * len(vals))
    colnames = ','.join([f'"{c}"' for c in cols_to_set])
    sql = f'INSERT INTO "{schema}"."{table}" ({colnames}) VALUES ({placeholders}) RETURNING *'
    with conn.cursor() as cur:
        cur.execute(sql, vals)
        row = cur.fetchone()
        conn.commit()
        return row


def pick_random_pk(conn, schema, table, pk_cols):
    if pk_cols:
        cols = ','.join([f'"{c}"' for c in pk_cols])
        q = f'SELECT {cols} FROM "{schema}"."{table}" ORDER BY random() LIMIT 1'
        with conn.cursor() as cur:
            cur.execute(q)
            r = cur.fetchone()
            return r
    else:
        q = f'SELECT ctid FROM "{schema}"."{table}" ORDER BY random() LIMIT 1'
        with conn.cursor() as cur:
            cur.execute(q)
            r = cur.fetchone()
            return r


def update_row(conn, schema, table, columns, pk_cols):
    pk = pick_random_pk(conn, schema, table, pk_cols)
    if not pk:
        return False
    non_pk_columns = [c for c, dt, n, d in columns if c not in pk_cols]
    if not non_pk_columns:
        return False
    n_updates = max(1, min(3, len(non_pk_columns)))
    chosen = random.sample(non_pk_columns, n_updates)
    set_parts = []
    vals = []
    colmap = {c: dt for c, dt, n, d in columns}
    for c in chosen:
        # For fund_metadata, sometimes set description/price to NULL on update
        if table == 'fund_metadata' and c in ('fund_description', 'fund_price') and random.random() < 0.25:
            set_parts.append(f'"{c}" = %s')
            vals.append(None)
            continue
        # For fund_unit.fund_code, sometimes pick an existing fund_metadata code
        if table == 'fund_unit' and c == 'fund_code':
            try:
                with conn.cursor() as cur:
                    cur.execute(f'SELECT fund_code FROM "{schema}"."fund_metadata" ORDER BY random() LIMIT 1')
                    r = cur.fetchone()
                    if r and random.random() < 0.8:
                        set_parts.append(f'"{c}" = %s')
                        vals.append(r[0])
                        continue
            except Exception:
                pass
        set_parts.append(f'"{c}" = %s')
        vals.append(gen_value(colmap[c], c))
    where_parts = []
    if pk_cols:
        for i, c in enumerate(pk_cols):
            where_parts.append(f'"{c}" = %s')
            vals.append(pk[i])
    else:
        # ctid
        where_parts.append('ctid = %s')
        vals.append(pk[0])
    sql = f'UPDATE "{schema}"."{table}" SET {", ".join(set_parts)} WHERE {" AND ".join(where_parts)} RETURNING *'
    with conn.cursor() as cur:
        try:
            cur.execute(sql, vals)
            row = cur.fetchone()
            conn.commit()
            return row
        except Exception:
            conn.rollback()
            return False


def delete_row(conn, schema, table, pk_cols):
    pk = pick_random_pk(conn, schema, table, pk_cols)
    if not pk:
        return False

    # If deleting from fund_metadata, ensure no fund_unit references exist for this fund_code
    if table == 'fund_metadata':
        # determine fund_code for this pk
        try:
            with conn.cursor() as cur:
                if pk_cols:
                    where = ' AND '.join([f'"{c}" = %s' for c in pk_cols])
                    cur.execute(f'SELECT fund_code FROM "{schema}"."{table}" WHERE {where} LIMIT 1', pk)
                else:
                    cur.execute(f'SELECT fund_code FROM "{schema}"."{table}" WHERE ctid = %s LIMIT 1', (pk[0],))
                row = cur.fetchone()
                fund_code_val = row[0] if row else None
                if fund_code_val:
                    cur.execute(f'SELECT 1 FROM "{schema}"."fund_unit" WHERE fund_code = %s LIMIT 1', (fund_code_val,))
                    if cur.fetchone():
                        # referenced; do not delete
                        return False
        except Exception:
            # on any error, avoid deleting to be safe
            return False

    vals = []
    if pk_cols:
        where_parts = []
        for i, c in enumerate(pk_cols):
            where_parts.append(f'"{c}" = %s')
            vals.append(pk[i])
    else:
        where_parts = ['ctid = %s']
        vals.append(pk[0])
    sql = f'DELETE FROM "{schema}"."{table}" WHERE {" AND ".join(where_parts)} RETURNING *'
    with conn.cursor() as cur:
        try:
            cur.execute(sql, vals)
            row = cur.fetchone()
            conn.commit()
            return row
        except Exception:
            conn.rollback()
            return False


def ensure_tables(conn, schema, tables):
    # Filter to existing tables in the schema
    q = """
    SELECT table_name FROM information_schema.tables
    WHERE table_schema=%s AND table_name = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, tables))
        rows = cur.fetchall()
        return [r[0] for r in rows]


def main():
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)
        Faker.seed(args.seed)
    conn = connect(args)
    target_tables = [
        'awsdms_ddl_audit',
        'fund_metadata',
        'fund_unit',
        'party',
        'policy',
    ]
    if args.tables:
        target_tables = [t.strip() for t in args.tables.split(',') if t.strip()]
    existing = ensure_tables(conn, args.schema, target_tables)
    if not existing:
        print('No target tables found in schema', args.schema)
        sys.exit(1)

    # Pre-fetch columns and pk info
    table_meta = {}
    for t in existing:
        cols = get_columns(conn, args.schema, t)
        pk = get_primary_key(conn, args.schema, t)
        table_meta[t] = {'columns': cols, 'pk': pk}

    ops = args.operations
    counts = {'insert': 0, 'update': 0, 'delete': 0}
    for i in range(ops):
        table = random.choice(list(table_meta.keys()))
        meta = table_meta[table]
        choice = random.random()
        # bias towards insert slightly if table empty
        if choice < 0.55:
            r = insert_row(conn, args.schema, table, meta['columns'])
            if r is not None:
                counts['insert'] += 1
        elif choice < 0.85:
            r = update_row(conn, args.schema, table, meta['columns'], meta['pk'])
            if r:
                counts['update'] += 1
        else:
            r = delete_row(conn, args.schema, table, meta['pk'])
            if r:
                counts['delete'] += 1
        if (i + 1) % 25 == 0:
            print(f'Progress: {i+1}/{ops}  inserts={counts["insert"]} updates={counts["update"]} deletes={counts["delete"]}')

    print('Done. Summary:', counts)


if __name__ == '__main__':
    main()
