/*
===============================================================================
                            Sales Report
===============================================================================
Purpose:
    - This report consolidates order-level transaction data with customer
      and product details for operational and shipping analysis.

Highlights:
    1. Joins fact_sales with dim_customers and dim_products
       to build a unified order-level dataset.
    2. Includes all transactional details:
       - order info, dates, amounts, quantity
    3. Calculates shipping KPIs:
       - shipping_delay_days (order → shipping)
       - delivery_window_days (shipping → due)
       - delivery_status (Late / On Time)
    4. Enriches with customer context:
       - segment, age_group, country, performance_tier
    5. Enriches with product context:
       - category, subcategory, product_line, product_segment
===============================================================================
*/
IF OBJECT_ID('gold.report_sales', 'V') IS NOT NULL
    DROP VIEW gold.report_sales;
GO

CREATE VIEW gold.report_sales AS

WITH base_query AS (
/*---------------------------------------------------------------------------
1) Base Query: Retrieves core columns from fact_sales, report_customers,
   and report_products
---------------------------------------------------------------------------*/
    SELECT
        -- Keys
        f.customer_key,
        f.product_key,

        -- Order Details
        f.order_number,
        f.order_date,
        f.shipping_date,
        f.due_date,
        f.sales_amount,
        f.quantity,
        f.price,

        -- Customer Details (from report_customers)
        c.customer_id,
        c.customer_number,
        c.full_name,
        c.country,
        c.gender,
        c.marital_status,
        c.age,
        c.age_group,
        c.customer_segment,
        c.performance_tier,

        -- Product Details (from report_products)
        p.product_id,
        p.product_number,
        p.product_name,
        p.category,
        p.subcategory,
        p.product_line,
        p.maintenance,
        p.cost,
        p.product_segment

    FROM gold.fact_sales f
    LEFT JOIN gold.report_customers c
        ON f.customer_key = c.customer_key
    LEFT JOIN gold.report_products p
        ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
)

/*---------------------------------------------------------------------------
2) Final Query: Adds calculated shipping KPIs
---------------------------------------------------------------------------*/
SELECT
    -- Keys (for Power BI relationships)
    customer_key,
    product_key,

    -- Order Info
    order_number,
    order_date,
    shipping_date,
    due_date,
    sales_amount,
    quantity,
    price,

    -- Customer Info
    customer_id,
    customer_number,
    full_name,
    country,
    gender,
    marital_status,
    age,
    age_group,
    customer_segment,
    performance_tier,

    -- Product Info
    product_id,
    product_number,
    product_name,
    category,
    subcategory,
    product_line,
    maintenance,
    cost,
    product_segment,

    -- Shipping KPIs
    DATEDIFF(DAY, order_date, shipping_date) AS shipping_delay_days,
    DATEDIFF(DAY, shipping_date, due_date)   AS delivery_window_days,
    CASE
        WHEN shipping_date > due_date THEN 'Late'
        ELSE 'On Time'
    END AS delivery_status

FROM base_query;
