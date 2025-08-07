import pandas as pd
from sqlalchemy import create_engine

excel_path = "../data/raw_data.xlsx"

orders_df = pd.read_excel(excel_path, sheet_name="orders")
customers_df = pd.read_excel(excel_path, sheet_name="customers")
products_df = pd.read_excel(excel_path, sheet_name="products")

db_user = "cbs_username"
db_password = "cbs_password"
db_host = "localhost"
db_port = "5432"
db_name = "coffee_bean_sales"

engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

orders_df.to_sql("orders", engine, if_exists="replace", index=False)
customers_df.to_sql("customers", engine, if_exists="replace", index=False)
products_df.to_sql("products", engine, if_exists="replace", index=False)

print(f"Data successfully moved to PostgreSQL from {excel_path}")
