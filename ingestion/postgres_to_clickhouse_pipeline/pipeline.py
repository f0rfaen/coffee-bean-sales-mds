import clickhouse_connect
import psycopg2
import toml


def extract_postgres_table(table_name: str):
    config = toml.load("config.toml")
    pg_cfg = config["postgres"]
    
    conn = psycopg2.connect(
        host=pg_cfg["host"],
        port=pg_cfg["port"],
        database=pg_cfg["database"],
        user=pg_cfg["username"],
        password=pg_cfg["password"],
    )
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name}")
    
    # Get the original column names from PostgreSQL
    original_columns = [desc[0] for desc in cur.description]
    
    # Transform the column names to lowercase and with underscores
    transformed_columns = [col.lower().replace(' ', '_') for col in original_columns]

    for row in cur.fetchall():
        # Yield the dictionary with the new column names
        yield {col: val for col, val in zip(transformed_columns, row)}

    cur.close()
    conn.close()

def create_and_run_pipeline():
    config = toml.load("config.toml")
    ch_cfg = config["clickhouse"]
    pipeline_cfg = config["pipeline"]
    tables = config["tables"]["source_tables"]

    # column schemas (optional – you can skip if you want dlt to infer)
    orders_columns = {
        "order_id": {"data_type": "String"},
        "order_date": {"data_type": "Date"},
        "customer_id": {"data_type": "String"},
        "product_id": {"data_type": "String"},
        "quantity": {"data_type": "Int64"},
        "customer_name": {"data_type": "Nullable(String)"},
        "email": {"data_type": "Nullable(String)"},
        "country": {"data_type": "Nullable(String)"},
        "coffee_type": {"data_type": "Nullable(String)"},
        "roast_type": {"data_type": "Nullable(String)"},
        "size": {"data_type": "Nullable(String)"},
        "unit_price": {"data_type": "Nullable(Float64)"},
        "sales": {"data_type": "Nullable(Float64)"},
    }

    customers_columns = {
        "customer_id": {"data_type": "String"},
        "customer_name": {"data_type": "String"},
        "email": {"data_type": "Nullable(String)"},
        "phone_number": {"data_type": "Nullable(String)"},
        "address_line_1": {"data_type": "Nullable(String)"},
        "city": {"data_type": "Nullable(String)"},
        "country": {"data_type": "Nullable(String)"},
        "postcode": {"data_type": "Nullable(String)"},
        "loyalty_card": {"data_type": "Nullable(String)"},
        "_dlt_load_id": {"data_type": "String"},
        "_dlt_id": {"data_type": "String"},
    }

    products_columns = {
        "product_id": {"data_type": "String"},
        "coffee_type": {"data_type": "String"},
        "roast_type": {"data_type": "String"},
        "size": {"data_type": "Nullable(Float64)"},
        "unit_price": {"data_type": "Nullable(Float64)"},
        "price_per_100g": {"data_type": "Nullable(Float64)"},
        "profit": {"data_type": "Nullable(Float64)"},
        "_dlt_load_id": {"data_type": "String"},
        "_dlt_id": {"data_type": "String"},
    }

    columns_map = {
        "orders": orders_columns,
        "customers": customers_columns,
        "products": products_columns,
    }

    client = clickhouse_connect.get_client(
        host=ch_cfg["host"],
        port=ch_cfg["port"],
        database=ch_cfg["database"],
        username=ch_cfg["username"],
        password=ch_cfg["password"],
        secure=False,
    )

    for table in tables:
        print(f"Processing table: {table}")
        
        # Get data and transform column names during extraction
        data = list(extract_postgres_table(table))
        if not data:
            print(f"No data found for table: {table}")
            continue

        # Get the new, transformed column names
        column_names = list(data[0].keys())
        
        columns_def = []
        for col_name in column_names:
            # Use the new lowercase/underscore names to find the data type
            map_key = col_name
            if map_key in columns_map[table]:
                # Use the new, clean column name for the ClickHouse schema
                columns_def.append(f'{col_name} {columns_map[table][map_key]["data_type"]}')
        columns_def_str = ", ".join(columns_def)
        client.command(f'DROP TABLE IF EXISTS {pipeline_cfg["dataset_name"]}.{table}')
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {pipeline_cfg["dataset_name"]}.{table} (
            {columns_def_str}
        ) ENGINE = MergeTree() ORDER BY tuple()
        """
        client.command(create_table_sql)
        
        # Insert data
        data_list_of_lists = [list(d.values()) for d in data]
        client.insert(f'{pipeline_cfg["dataset_name"]}.{table}', data_list_of_lists, column_names=column_names)
        print(f"Successfully loaded {len(data)} rows into table: {table}")


if __name__ == "__main__":
    create_and_run_pipeline()
