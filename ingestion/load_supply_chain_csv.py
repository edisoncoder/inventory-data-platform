import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

CSV_PATH = Path("data/supply_chain_data.csv")


def extract_csv() -> pd.DataFrame:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    df = df.rename(columns={
        "Product type": "product_type",
        "SKU": "sku",
        "Price": "price",
        "Availability": "availability",
        "Number of products sold": "number_products_sold",
        "Revenue generated": "revenue_generated",
        "Customer demographics": "customer_demographics",
        "Stock levels": "stock_levels",
        "Lead times": "lead_times",
        "Order quantities": "order_quantities",
        "Shipping times": "shipping_times",
        "Shipping carriers": "shipping_carriers",
        "Shipping costs": "shipping_costs",
        "Supplier name": "supplier_name",
        "Location": "location",
        "Lead time": "lead_time",
        "Production volumes": "production_volumes",
        "Manufacturing lead time": "manufacturing_lead_time",
        "Manufacturing costs": "manufacturing_costs",
        "Inspection results": "inspection_results",
        "Defect rates": "defect_rates",
        "Transportation modes": "transportation_modes",
        "Routes": "routes",
        "Costs": "costs",
    })

    return df


def transform_types(df: pd.DataFrame) -> pd.DataFrame:
    int_columns = [
        "availability",
        "number_products_sold",
        "stock_levels",
        "lead_times",
        "order_quantities",
        "shipping_times",
        "lead_time",
        "production_volumes",
        "manufacturing_lead_time",
    ]

    float_columns = [
        "price",
        "revenue_generated",
        "shipping_costs",
        "manufacturing_costs",
        "defect_rates",
        "costs",
    ]

    for col in int_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in float_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def load_snapshot(df: pd.DataFrame, engine) -> None:
    with engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE raw.supply_chain_data"))

    df.to_sql(
        name="supply_chain_data",
        con=engine,
        schema="raw",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )


def main():
    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

    df = extract_csv()
    df = transform_types(df)

    print(f"Linhas lidas do CSV: {len(df)}")
    print("Iniciando carga snapshot idempotente...")

    load_snapshot(df, engine)

    print("Carga concluída com sucesso.")


if __name__ == "__main__":
    main()