/*
===============================================================================
                            Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics, behaviors, and
      advanced analytics for business decision-making.

Highlights:
    1. Joins transaction data with product details to build
       a unified base dataset.
    2. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    3. Segments products by revenue performance:
       - High-Performer (> 50,000)
       - Mid-Range     (>= 10,000)
       - Low-Performer (< 10,000)
    4. Calculates core KPIs:
       - recency (months since last sale)
       - average selling price
       - average order revenue (float-safe division)
       - average monthly revenue (float-safe division)
===============================================================================
*/
IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
    DROP VIEW gold.report_products;
GO

CREATE VIEW gold.report_products AS

WITH base_query AS (
/*---------------------------------------------------------------------------
1) Base Query: Retrieves core columns from fact_sales and dim_products
---------------------------------------------------------------------------*/
    SELECT
        f.order_number,
        f.order_date,
        f.shipping_date,        
        f.due_date,             
        f.customer_key,
        f.sales_amount,
        f.quantity,
        f.price,                
        p.product_key,
        p.product_id,           
        p.product_number,       
        p.product_name,
        p.category_id,          
        p.category,
        p.subcategory,
        p.maintenance,          
        p.cost,
        p.product_line,         
        p.start_date            
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p
        ON f.product_key = p.product_key
    WHERE order_date IS NOT NULL
),

product_aggregations AS (
/*---------------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the product level
---------------------------------------------------------------------------*/
    SELECT
        product_key,
        product_id,            
        product_number,      
        product_name,
        category_id,           
        category,
        subcategory,
        maintenance,          
        cost,
        product_line,           
        start_date,             
        DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
        MAX(order_date)  AS last_sale_date,
        COUNT(DISTINCT order_number) AS total_orders,
        COUNT(DISTINCT customer_key) AS total_customers,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity,
        ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1) AS avg_selling_price
    FROM base_query
    GROUP BY
        product_key,
        product_id,             
        product_number,         
        product_name,
        category_id,            
        category,
        subcategory,
        maintenance,            
        cost,
        product_line,           
        start_date              
)

/*---------------------------------------------------------------------------
3) Final Query: Combines all product results into one output
---------------------------------------------------------------------------*/
SELECT
    product_key,
    product_id,                 
    product_number,             
    product_name,
    category_id,                
    category,
    subcategory,
    maintenance,                
    cost,
    product_line,               
    start_date,                 
    last_sale_date,
    DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
    CASE
        WHEN total_sales > 50000  THEN 'High-Performer'
        WHEN total_sales >= 10000 THEN 'Mid-Range'
        ELSE 'Low-Performer'
    END AS product_segment,
    lifespan,
    total_orders,
    total_sales,
    total_quantity,
    total_customers,
    avg_selling_price,
    -- Average Order Revenue (AOR)
    CASE
        WHEN total_orders = 0 THEN 0
        ELSE total_sales * 1.0 / total_orders   
    END AS avg_order_revenue,
    -- Average Monthly Revenue
    CASE
        WHEN lifespan = 0 THEN total_sales
        ELSE total_sales * 1.0 / lifespan       
    END AS avg_monthly_revenue
FROM product_aggregations;
