import dlt
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
    columns = [desc[0] for desc in cur.description]

    for row in cur.fetchall():
        yield {col: val for col, val in zip(columns, row)}

    cur.close()
    conn.close()


def create_and_run_pipeline():
    config = toml.load("config.toml")
    duck_cfg = config["duckdb"]
    pipeline_cfg = config["pipeline"]
    tables = config["tables"]["source_tables"]

    orders_columns = {
        "customer_name": {"data_type": "text"},
        "email": {"data_type": "text"},
        "country": {"data_type": "text"},
        "coffee_type": {"data_type": "text"},
        "roast_type": {"data_type": "text"},
        "size": {"data_type": "text"},
        "unit_price": {"data_type": "decimal"},
        "sales": {"data_type": "decimal"},
        "order_id": {"data_type": "text"},
        "order_date": {"data_type": "date"},
        "customer_id": {"data_type": "text"},
        "product_id": {"data_type": "text"},
        "quantity": {"data_type": "bigint"},
        "_dlt_load_id": {"data_type": "text"},
        "_dlt_id": {"data_type": "text"},
    }
    
    customers_columns = {
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
    }

    products_columns = {
        "product_id": {"data_type": "text"},
        "coffee_type": {"data_type": "text"},
        "roast_type": {"data_type": "text"},
        "size": {"data_type": "double"},
        "unit_price": {"data_type": "double"},
        "price_per_100g": {"data_type": "double"},
        "profit": {"data_type": "double"},
        "_dlt_load_id": {"data_type": "text"},
        "_dlt_id": {"data_type": "text"},
    }

    columns_map = {
        "orders": orders_columns,
        "customers": customers_columns,
        "products": products_columns,
    }

    pipeline_instance = dlt.pipeline(
        pipeline_name=pipeline_cfg["name"],
        destination=dlt.destinations.duckdb(duck_cfg["file_path"]),
        dataset_name=pipeline_cfg["dataset_name"],
    )

    resources = [
        dlt.resource(
            extract_postgres_table,
            name=table,
            columns=columns_map.get(table, None)
        )(table) for table in tables
    ]

    load_info = pipeline_instance.run(resources)
    print(load_info)


if __name__ == "__main__":
    create_and_run_pipeline()
