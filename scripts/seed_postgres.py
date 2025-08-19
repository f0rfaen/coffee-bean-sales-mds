import pandas as pd
from sqlalchemy import create_engine

excel_path = "../data/raw_data.xlsx"

db_user = "cbs_username"
db_password = "cbs_password"
db_host = "localhost"
db_port = "5433"
db_name = "coffee_bean_sales"

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

sheets = [
    ("orders", "orders"),
    ("customers", "customers"),
    ("products", "products")
]

print("Seeding PostgreSQL")
for sheet_name, table_name in sheets:
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name)        
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        
        print(f"Sheet: '{sheet_name}' to Table: '{table_name}' Sucessfully created.")
    except ValueError as e:
        print(f"Warning: Sheet '{sheet_name}' not found in the Excel file. Skipping this sheet.")
    except Exception as e:
        print(f"An error occurred while processing sheet '{sheet_name}': {e}")


print("All sheets are successfully moved to PostgreSQL")