import dlt
from dlt.sources.helpers import requests
import psycopg2
import toml
import os
from typing import Iterator, Dict, Any, Optional, List
from datetime import datetime


def inspect_postgres_database() -> Dict[str, Any]:
    config_file = "config_local.toml" if os.environ.get("RUN_LOCAL") else "config_docker.toml"
    config = toml.load(config_file)
    pg_cfg = config["postgres"]
    
    try:
        conn = psycopg2.connect(
            host=pg_cfg["host"],
            port=pg_cfg["port"],
            database=pg_cfg["database"],
            user=pg_cfg["username"],
            password=pg_cfg["password"],
        )
        cur = conn.cursor()
        
        print(f"Connected to PostgreSQL database: {pg_cfg['database']} at {pg_cfg['host']}:{pg_cfg['port']}")
        
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        tables = [row[0] for row in cur.fetchall()]
        print(f"\nAvailable tables: {tables}")
        
        table_info = {}
        for table in tables:
            cur.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position
            """, (table,))
            
            columns = cur.fetchall()
            table_info[table] = {
                'columns': [{'name': col[0], 'type': col[1], 'nullable': col[2]} for col in columns],
                'column_count': len(columns)
            }
            
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cur.fetchone()[0]
            table_info[table]['row_count'] = row_count
            
            print(f"\nTable: {table}")
            print(f"Row count: {row_count}")
            print(f"Columns: {[col['name'] for col in table_info[table]['columns']]}")
        
        cur.close()
        conn.close()
        
        return table_info
        
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {str(e)}")
        print(f"Connection details: host={pg_cfg['host']}, port={pg_cfg['port']}, database={pg_cfg['database']}, user={pg_cfg['username']}")
        raise


def normalize_column_name(col_name: str) -> str:
    return col_name.lower().replace(' ', '_')


def extract_postgres_table_incremental(
    table_name: str, 
    cursor_column: Optional[str] = None,
    primary_key: Optional[str] = None
):
    config_file = "config_local.toml" if os.environ.get("RUN_LOCAL") else "config_docker.toml"
    config = toml.load(config_file)
    pg_cfg = config["postgres"]
    
    conn = psycopg2.connect(
        host=pg_cfg["host"],
        port=pg_cfg["port"],
        database=pg_cfg["database"],
        user=pg_cfg["username"],
        password=pg_cfg["password"],
    )
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            )
        """, (table_name,))
        
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            return
        
        if cursor_column:
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s 
                AND column_name = %s
            """, (table_name, cursor_column))
            
            cursor_exists = cur.fetchone()
            if not cursor_exists:
                print(f"Warning: Cursor column '{cursor_column}' does not exist in table '{table_name}'. Using full extraction.")
                cursor_column = None
        
        if cursor_column:
            cursor_normalized = normalize_column_name(cursor_column)
            last_cursor_value = dlt.current.source_state().get(f"{table_name}_{cursor_normalized}_last_cursor")
            
            quoted_cursor_column = f'"{cursor_column}"'
            quoted_table_name = f'"{table_name}"' if ' ' in table_name else table_name
            
            if last_cursor_value:
                query = f"SELECT * FROM {quoted_table_name} WHERE {quoted_cursor_column} > %s ORDER BY {quoted_cursor_column}"
                cur.execute(query, (last_cursor_value,))
            else:
                query = f"SELECT * FROM {quoted_table_name} ORDER BY {quoted_cursor_column}"
                cur.execute(query)
        else:
            quoted_table_name = f'"{table_name}"' if ' ' in table_name else table_name
            cur.execute(f"SELECT * FROM {quoted_table_name}")
        
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        
        print(f"Extracted {len(rows)} rows from table '{table_name}'")
        
        max_cursor_value = None
        
        for row in rows:
            row_dict_original = {col: val for col, val in zip(columns, row)}
            
            row_dict = {}
            for col, val in zip(columns, row):
                normalized_col = normalize_column_name(col)
                row_dict[normalized_col] = val
            
            if cursor_column and cursor_column in row_dict_original:
                cursor_value = row_dict_original[cursor_column]
                if max_cursor_value is None or cursor_value > max_cursor_value:
                    max_cursor_value = cursor_value
            
            yield row_dict
        
        if cursor_column and max_cursor_value is not None:
            cursor_normalized = normalize_column_name(cursor_column)
            dlt.current.source_state()[f"{table_name}_{cursor_normalized}_last_cursor"] = max_cursor_value
            print(f"Saved cursor value for '{table_name}.{cursor_normalized}': {max_cursor_value}")
            
    except Exception as e:
        print(f"Error extracting from table '{table_name}': {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()


def create_and_run_pipeline():
    config_file = "config_local.toml" if os.environ.get("RUN_LOCAL") else "config_docker.toml"
    config = toml.load(config_file)
    duck_cfg = config["duckdb"]
    pipeline_cfg = config["pipeline"]
    tables = config["tables"]["source_tables"]

    table_configs = {
        "orders": {
            "columns": {
                "order_id": {"data_type": "text"},
                "order_date": {"data_type": "date"},
                "customer_id": {"data_type": "text"},
                "product_id": {"data_type": "text"},
                "quantity": {"data_type": "bigint"},
                "customer_name": {"data_type": "text"},  
                "email": {"data_type": "text"}, 
                "country": {"data_type": "text"}, 
                "coffee_type": {"data_type": "text"}, 
                "roast_type": {"data_type": "text"}, 
                "size": {"data_type": "double"},
                "unit_price": {"data_type": "double"},
                "sales": {"data_type": "double"},
                "_dlt_load_id": {"data_type": "text"},
                "_dlt_id": {"data_type": "text"},
            },
            "primary_key": "order_id", 
            "cursor_column": "Order Date", 
            "cursor_column_normalized": "order_date", 
        },
        "customers": {
            "columns": {
                "customer_id": {"data_type": "text"},
                "customer_name": {"data_type": "text"},
                "email": {"data_type": "text"},
                "phone_number": {"data_type": "text"},
                "address_line_1": {"data_type": "text"},
                "city": {"data_type": "text"},
                "country": {"data_type": "text"},
                "postcode": {"data_type": "text"},
                "loyalty_card": {"data_type": "text"},
                "_dlt_load_id": {"data_type": "text"},
                "_dlt_id": {"data_type": "text"},
            },
            "primary_key": "customer_id",
            "cursor_column": None,
        },
        "products": {
            "columns": {
                "product_id": {"data_type": "text"},
                "coffee_type": {"data_type": "text"},
                "roast_type": {"data_type": "text"},
                "size": {"data_type": "double"},
                "unit_price": {"data_type": "double"},
                "price_per_100g": {"data_type": "double"},
                "profit": {"data_type": "double"},
                "_dlt_load_id": {"data_type": "text"},
                "_dlt_id": {"data_type": "text"},
            },
            "primary_key": "product_id",
            "cursor_column": None,
        }
    }

    pipeline_instance = dlt.pipeline(
        pipeline_name=pipeline_cfg["name"],
        destination=dlt.destinations.duckdb(
            credentials=duck_cfg["file_path"],
            create_indexes=True
        ),
        dataset_name="raw", 
    )

    resources = []
    for table in tables:
        if table not in table_configs:
            print(f"Warning: No configuration found for table {table}, skipping.")
            continue
            
        config = table_configs[table]
        
        if config.get("cursor_column"):
            write_disposition = "append"
        else:
            write_disposition = "replace"
        
        resource = dlt.resource(
            extract_postgres_table_incremental,
            name=table,
            columns=config["columns"],
            primary_key=config["primary_key"],
            write_disposition=write_disposition
        )(
            table, 
            cursor_column=config.get("cursor_column"),
            primary_key=config["primary_key"]
        )
        
        resources.append(resource)

    load_info = pipeline_instance.run(resources)
    print("\nPipeline load info:\n", load_info)


if __name__ == "__main__":
    try:
        table_info = inspect_postgres_database()
        
        config_file = "config_local.toml" if os.environ.get("RUN_LOCAL") else "config_docker.toml"
        config = toml.load(config_file)
        expected_tables = config["tables"]["source_tables"]
        
        available_tables = list(table_info.keys())
        missing_tables = [table for table in expected_tables if table not in available_tables]
        
        if missing_tables:
            print(f"Missing tables: {missing_tables}")
        else:
            print("\nAll expected tables found!")
            
            print(f"\nRUNNING PIPELINE: {config['pipeline']['name']}")
            create_and_run_pipeline()
            print(f"\nPIPELINE: {config['pipeline']['name']} COMPLETED")
            
    except Exception as e:
        print(f"Failed to extract data: {str(e)}")