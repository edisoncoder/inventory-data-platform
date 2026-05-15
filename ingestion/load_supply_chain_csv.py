import pandas as pd

EXPECTED_COLUMNS = [
    'product_type', 'sku', 'price', 'availability', 'number_of_products_sold', 'revenue_generated',
    'customer_demographics', 'stock_levels', 'lead_times', 'order_quantities', 'shipping_times',
    'shipping_carriers', 'shipping_costs', 'supplier_name', 'location', 'lead_time',
    'production_volumes', 'manufacturing_lead_time', 'manufacturing_costs', 'inspection_results',
    'defect_rates', 'transportation_modes', 'routes', 'costs'
]

TEXT_COLUMNS = [
    'product_type', 'sku', 'customer_demographics', 'shipping_carriers', 'supplier_name',
    'location', 'inspection_results', 'transportation_modes', 'routes'
]

NUMERIC_COLUMNS = [
    'price', 'availability', 'number_of_products_sold', 'revenue_generated', 'stock_levels',
    'lead_times', 'order_quantities', 'shipping_times', 'shipping_costs', 'lead_time',
    'production_volumes', 'manufacturing_lead_time', 'manufacturing_costs', 'defect_rates', 'costs'
]

IDENTIFIER_COLUMNS = ['sku']

def load_supply_chain_csv(csv_path: str) -> pd.DataFrame:
    """
    Carrega o arquivo CSV da cadeia de suprimentos, valida as 24 colunas esperadas,
    converte tipos de dados apropriados e retorna um DataFrame limpo.
    """
    df = pd.read_csv(csv_path)
    df = df.rename(columns={
            "Product type": "product_type",
            "SKU": "sku",
            "Price": "price",
            "Availability": "availability",
            "Number of products sold": "number_of_products_sold",
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
    # Verifica se todas as colunas esperadas estão presentes
    missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Colunas ausentes no CSV: {missing_cols}")

    # Seleciona apenas as colunas esperadas
    df = df[EXPECTED_COLUMNS]

    # Converte colunas de texto para string
    for col in TEXT_COLUMNS:
        df[col] = df[col].astype(str)

    # Converte colunas numéricas, tratando erros como NaN
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # TODO v0.3.0: Implementar carga incremental baseada em SKU ou timestamp

    return df

