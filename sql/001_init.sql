CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS raw.supply_chain_data (
    product_type TEXT,
    sku TEXT,
    price NUMERIC,
    availability INTEGER,
    number_products_sold INTEGER,
    revenue_generated NUMERIC,
    customer_demographics TEXT,
    stock_levels INTEGER,
    lead_times INTEGER,
    order_quantities INTEGER,
    shipping_times INTEGER,
    shipping_carriers TEXT,
    shipping_costs NUMERIC,
    supplier_name TEXT,
    location TEXT,
    lead_time INTEGER,
    production_volumes INTEGER,
    manufacturing_lead_time INTEGER,
    manufacturing_costs NUMERIC,
    inspection_results TEXT,
    defect_rates NUMERIC,
    transportation_modes TEXT,
    routes TEXT,
    costs NUMERIC,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);