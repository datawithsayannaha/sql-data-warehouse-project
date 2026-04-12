/* ADVANCED DATA ANALYSIS USING SQL (WINDOW FUNCTIONS + TIME SERIES + SEGMENTATION + PERFORMANCE ANALYSIS) */

SELECT
YEAR (order_date) as order_year,
MONTH(order_date) as order_month,
SUM(sales_amount) as total_Sales,
COUNT (DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR (order_date), MONTH (order_date)
ORDER BY YEAR (order_date), MONTH (order_date)

SELECT
DATETRUNC (month, order_date) as order_date,
SUM(sales_amount) as total_Sales,
COUNT (DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC (month, order_date)
ORDER BY DATETRUNC (month, order_date)

--cummulative analysis
--Calculate the total sales per month
--and the running total of sales over time
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (PARTITION BY order_date ORDER BY order_date) AS running_total_sales,
AVG(avg_price) OVER (ORDER BY order_date) AS moving_average_price
FROM
(
SELECT
DATETRUNC(month, order_date) AS order_date,
SUM(sales_amount) AS total_sales,
AVG(price) as avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
)t

--Performance analysis

/* Analyze the yearly performance of products by comparing 
their sales to both the average sales performance of the product and the previous year's sales */
WITH yearly_product_sales AS (
    SELECT
        YEAR(f.order_date) AS order_year, 
        p.product_name,
        SUM(f.sales_amount) AS current_sales
    FROM gold.fact_sales f
    LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
    WHERE f.order_date IS NOT NULL
    GROUP BY YEAR(f.order_date), p.product_name
)
SELECT
    order_year,
    product_name,
    current_sales,
    AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
    current_sales - AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,
    CASE 
        WHEN current_sales > AVG(current_sales) OVER (PARTITION BY product_name) THEN 'Above Avg' 
        WHEN current_sales < AVG(current_sales) OVER (PARTITION BY product_name) THEN 'Below Avg'
        ELSE 'Avg'
    END AS avg_change,
LAG(current_sales) over (PARTITION BY product_name ORDER BY order_year) py_sales,
current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
CASE WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase' 
     WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease' 
     ELSE 'No Change'  
END AS py_change
FROM yearly_product_sales
ORDER BY product_name, order_year;

-- PART TO WHOLE

--Which categories contribute the most to overall sales?
WITH category_sales AS(
SELECT
category,
SUM(sales_amount) total_sales
from gold.fact_sales f
LEFT JOIN gold.dim_products p
on p.product_key = f.product_key
GROUP BY category
)
SELECT
category,
total_sales,
SUM(total_sales) OVER () Overall_sales,
CONCAT(ROUND((CAST(total_sales AS FLOAT)/SUM(total_sales) OVER()) *100,2),'%') AS percentage_of_total
from category_sales
ORDER BY total_sales DESC

-- Data Segmentation
/* Segment products into cost ranges and count how many products far into each segment */
WITH product_segments AS (
SELECT
product_key,
product_name,
cost,
CASE WHEN cost <100 THEN 'BELOW 100'
    WHEN cost BETWEEN 100 AND 500 Then '100-500'
    WHEN cost BETWEEN 500 AND 1000 Then '500-1000'
    ELSE 'Above 1000'
END cost_range
FROM gold.dim_products)
SELECT
cost_range,
COUNT (product_key) AS total_products
FROM product_segments
GROUP BY cost_range
ORDER BY total_products DESC



