/*
===============================================================================
                            Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics, behaviors, and 
      advanced analytics for business decision-making.

Highlights:
    1. Joins transaction data with customer demographics to build
       a unified base dataset.
    2. Aggregates customer-level metrics:
       - total orders
       - total sales
       - total quantity purchased
       - total products
       - lifespan (in months)
    3. Segments customers into categories (VIP, Regular, New)
       and age groups (Under 20, 20-29, 30-39, 40-49, 50 and above).
    4. Calculates core KPIs:
       - recency (months since last order)
       - average order value (float-safe division)
       - average monthly spend (float-safe division)
    5. Applies advanced window function analytics:
       - revenue rank & percentile across all customers
       - segment-level average sales benchmarking
       - variance from segment average (above/below)
       - cumulative sales running total (Pareto ready)
       - individual market share percentage
       - z-score for statistical outlier detection
       - performance tiering (Elite / Mid / Low)
       - segment density (customer count per segment)
===============================================================================
*/

IF OBJECT_ID('gold.report_customers', 'V') IS NOT NULL
    DROP VIEW gold.report_customers;
GO

CREATE VIEW gold.report_customers AS

WITH base_query AS (
    SELECT
        f.order_number,
        f.product_key,
        f.order_date,
        f.shipping_date,        
        f.due_date,             
        f.sales_amount,
        f.quantity,
        f.price,                
        c.customer_key,
        c.customer_id,          
        c.customer_number,
        c.full_name,
        c.country,              
        c.marital_status,       
        c.gender,               
        c.create_date,          
        DATEDIFF(year, c.birthdate, GETDATE()) AS age
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_customers c
        ON c.customer_key = f.customer_key
    WHERE order_date IS NOT NULL
),

customer_aggregation AS (
    SELECT
        customer_key,
        customer_id,            
        customer_number,
        full_name,
        country,                
        marital_status,         
        gender,                 
        create_date,            
        age,
        COUNT(DISTINCT order_number)  AS total_orders,
        SUM(sales_amount)             AS total_sales,
        SUM(quantity)                 AS total_quantity,
        COUNT(DISTINCT product_key)   AS total_products,
        MAX(order_date)               AS last_order_date,
        DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
    FROM base_query
    GROUP BY
        customer_key,
        customer_id,           
        customer_number,
        full_name,
        country,                
        marital_status,         
        gender,                 
        create_date,            
        age
),

customer_metrics AS (
    SELECT
        customer_key,
        customer_id,            
        customer_number,
        full_name,
        country,                
        marital_status,         
        gender,                 
        create_date,            
        age,
        CASE
            WHEN age < 20 THEN 'Under 20'
            WHEN age BETWEEN 20 AND 29 THEN '20-29'
            WHEN age BETWEEN 30 AND 39 THEN '30-39'
            WHEN age BETWEEN 40 AND 49 THEN '40-49'
            ELSE '50 and above'
        END AS age_group,
        CASE
            WHEN lifespan >= 12 AND total_sales > 5000  THEN 'VIP'
            WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
            ELSE 'New'
        END AS customer_segment,
        last_order_date,
        DATEDIFF(month, last_order_date, GETDATE()) AS recency,
        total_orders,
        total_sales,
        total_quantity,
        total_products,
        lifespan,
        CASE WHEN total_orders = 0 THEN 0
             ELSE total_sales * 1.0 / total_orders
        END AS avg_order_value,
        CASE WHEN lifespan = 0 THEN total_sales
             ELSE total_sales * 1.0 / lifespan
        END AS avg_monthly_spend
    FROM customer_aggregation
)

SELECT
    customer_key,
    customer_id,            
    customer_number,
    full_name,
    country,                
    marital_status,         
    gender,                 
    create_date,            
    age,
    age_group,
    customer_segment,
    last_order_date,
    recency,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    lifespan,
    avg_order_value,
    avg_monthly_spend,

    -- 1. Ranking & Percentile
    RANK()         OVER (ORDER BY total_sales DESC) AS revenue_rank,
    PERCENT_RANK() OVER (ORDER BY total_sales DESC) AS revenue_percentile,

    -- 2. Segment Benchmarking
    AVG(total_sales) OVER (PARTITION BY customer_segment) AS segment_avg_sales,
    total_sales - AVG(total_sales) OVER (PARTITION BY customer_segment)  AS vs_segment_avg,

    -- 3. Running Total Contribution
    SUM(total_sales) OVER (
        ORDER BY total_sales DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_sales,

    -- 4. Market Share %
    ROUND(total_sales * 100.0 / SUM(total_sales) OVER(), 4)AS market_share_percentage,

    -- 5. Statistical Outlier (Z-Score)
    (total_sales - AVG(total_sales) OVER())
        / NULLIF(STDEV(total_sales) OVER(), 0) AS sales_zscore,

    -- 6. Performance Tiering
    CASE
        WHEN PERCENT_RANK() OVER (ORDER BY total_sales) >= 0.9 THEN 'Elite'
        WHEN PERCENT_RANK() OVER (ORDER BY total_sales) >= 0.5 THEN 'Mid'
        ELSE 'Low'
    END AS performance_tier,

    -- 7. Segment Density
    COUNT(*) OVER (PARTITION BY customer_segment) AS segment_size

FROM customer_metrics;
