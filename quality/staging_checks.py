import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

def run_check(check_name: str, query: str) -> bool:
    df = pd.read_sql(query, engine)
    invalid_rows = int(df.iloc[0]["invalid_rows"])

    if invalid_rows > 0:
        print(f"[ERRO] {check_name}: {invalid_rows} registros inválidos")
        return False

    print(f"[OK] {check_name}")
    return True

def main():
    print("Rodando validações da camada staging...\n")

    checks = [
        (
            "SKU nulo na staging",
            """
            SELECT COUNT(*) AS invalid_rows
            FROM staging.supply_chain_clean
            WHERE sku IS NULL
            """
        ),
        (
            "Preço negativo na staging",
            """
            SELECT COUNT(*) AS invalid_rows
            FROM staging.supply_chain_clean
            WHERE price < 0
            """
        ),
        (
            "Frete negativo na staging",
            """
            SELECT COUNT(*) AS invalid_rows
            FROM staging.supply_chain_clean
            WHERE shipping_costs < 0
            """
        ),
    ]

    failed_checks = []

    for name, query in checks:
        ok = run_check(name, query)
        if not ok:
            failed_checks.append(name)

    if failed_checks:
        raise Exception(
            "Falha nas validações da staging: " + "; ".join(failed_checks)
        )

    print("\nValidações da staging concluídas com sucesso.")

if __name__ == "__main__":
    main()