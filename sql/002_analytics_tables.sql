DROP TABLE IF EXISTS analytics.product_metrics;
DROP TABLE IF EXISTS analytics.supplier_metrics;
DROP TABLE IF EXISTS analytics.logistics_metrics;

DROP VIEW IF EXISTS analytics.product_metrics;
DROP VIEW IF EXISTS analytics.supplier_metrics;
DROP VIEW IF EXISTS analytics.logistics_metrics;

CREATE OR REPLACE VIEW analytics.product_metrics AS
SELECT
    product_type,
    COUNT(*) total_orders,
    SUM(number_products_sold) total_products_sold,
    SUM(revenue_generated) total_revenue,
    AVG(price) avg_price,
    AVG(defect_rates) avg_defect_rate
FROM raw.supply_chain_data
GROUP BY product_type;

CREATE OR REPLACE VIEW analytics.supplier_metrics AS
SELECT
    supplier_name,
    location,
    COUNT(*) total_orders,
    SUM(revenue_generated) total_revenue,
    AVG(shipping_costs) avg_shipping_cost,
    AVG(defect_rates) avg_defect_rate
FROM raw.supply_chain_data
GROUP BY supplier_name, location;

CREATE OR REPLACE VIEW analytics.logistics_metrics AS
SELECT
    transportation_modes,
    routes,
    AVG(shipping_times) avg_shipping_time,
    AVG(shipping_costs) avg_shipping_cost
FROM raw.supply_chain_data
GROUP BY transportation_modes, routes;