-- Dim product

CREATE OR REPLACE VIEW analytics.dim_product AS
SELECT DISTINCT
    sku,
    product_type
FROM raw.supply_chain_data;


-- Dim supplier

CREATE OR REPLACE VIEW analytics.dim_supplier AS
SELECT DISTINCT
    supplier_name,
    location
FROM raw.supply_chain_data;

-- Dim location

CREATE OR REPLACE VIEW analytics.dim_location AS
SELECT DISTINCT
    location
FROM raw.supply_chain_data;

-- Fact 

CREATE OR REPLACE VIEW analytics.fact_supply_chain AS
SELECT
    sku,
    supplier_name,
    location,
    transportation_modes,
    routes,

    -- métricas
    number_products_sold,
    revenue_generated,
    price,
    shipping_costs,
    manufacturing_costs,
    defect_rates,
    stock_levels

FROM raw.supply_chain_data;