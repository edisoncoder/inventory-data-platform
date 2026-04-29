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
    print("Rodando validações de qualidade...\n")

    checks = [
        (
            "Nulls em colunas críticas",
            """
            SELECT COUNT(*) AS invalid_rows
            FROM raw.supply_chain_data
            WHERE product_type IS NULL
               OR sku IS NULL
            """
        ),
        (
            "Tabela raw vazia",
            """
            SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS invalid_rows
            FROM raw.supply_chain_data
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
            "Falha nas validações de qualidade: " + "; ".join(failed_checks)
        )

    print("\nValidações concluídas com sucesso.")

if __name__ == "__main__":
    main()