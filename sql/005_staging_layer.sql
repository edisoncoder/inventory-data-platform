CREATE SCHEMA IF NOT EXISTS staging;

CREATE OR REPLACE VIEW staging.supply_chain_clean AS
SELECT
    sku,
    product_type,
    supplier_name,
    location,
    transportation_modes,
    routes,

    -- limpeza básica
    CASE 
        WHEN price < 0 THEN NULL
        ELSE price
    END AS price,

    CASE 
        WHEN shipping_costs < 0 THEN NULL
        ELSE shipping_costs
    END AS shipping_costs,

    CASE 
        WHEN manufacturing_costs < 0 THEN NULL
        ELSE manufacturing_costs
    END AS manufacturing_costs,

    number_products_sold,
    revenue_generated,
    defect_rates,
    stock_levels

FROM raw.supply_chain_data;


-- Atualize suas views analytics
CREATE OR REPLACE VIEW analytics.fact_supply_chain AS
SELECT
    sku,
    supplier_name,
    location,
    transportation_modes,
    routes,

    number_products_sold,
    revenue_generated,
    price,
    shipping_costs,
    manufacturing_costs,
    defect_rates,
    stock_levels

FROM staging.supply_chain_clean;

-- Teste
SELECT *
FROM staging.supply_chain_clean
WHERE price IS NULL;