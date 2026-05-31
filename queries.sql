-- queries.sql
-- data warehousing queries sql file
SET search_path to WalmartDW;

-- Q1. Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down 
-- [Identifies the top 5 products by revenue, split by weekdays and weekends, with monthly breakdowns for a year]

SELECT p.product_id, p.product_category, d.monthNum, d.is_weekend, SUM(s.sales_amount) AS total_revenue FROM walmartdw.sales s
JOIN walmartdw.product p ON s.product_id = p.product_id
JOIN walmartdw.date d ON s.date_id = d.date_id
WHERE d.year = 2017     
GROUP BY p.product_id, p.product_category, d.monthNum, d.is_weekend
ORDER BY d.monthNum, d.is_weekend, total_revenue DESC
LIMIT 5;

-- Q2. Customer Demographics by Purchase Amount with City Category Breakdown
-- Analyzes total purchase amounts by gender and age, detailed by city category.

SELECT c.gender, c.age_group, c.city_category, SUM(s.sales_amount) AS total_revenue, SUM(s.quantity) AS units_sold FROM walmartdw.sales s
JOIN walmartdw.customer c ON s.customer_id = c.customer_id
GROUP BY c.gender, c.age_group, c.city_category
ORDER BY c.city_category, c.gender, c.age_group;

-- Q3. Product Category Sales by Occupation
-- Examines total sales for each product category based on customer occupation.
SELECT p.product_category, c.occupation, SUM(s.sales_amount) AS total_revenue, SUM(s.quantity) AS units_sold FROM walmartdw.sales s
JOIN walmartdw.product p ON s.product_id = p.product_id
JOIN walmartdw.customer c ON s.customer_id = c.customer_id
GROUP BY p.product_category, c.occupation
ORDER BY p.product_category, total_revenue DESC;

-- Q4. Total Purchases by Gender and Age Group with Quarterly Trend
-- Tracks purchase amounts by gender and age across quarterly periods for the current year.
WITH latest_year AS (
    SELECT MAX(year) AS yr FROM walmartdw.date
)
SELECT d.quarter_num, c.gender, c.age_group, SUM(s.sales_amount) AS total_revenue, SUM(s.quantity) AS units_sold FROM walmartdw.sales s
JOIN walmartdw.date d ON s.date_id = d.date_id
JOIN walmartdw.customer c ON s.customer_id = c.customer_id
JOIN latest_year ON d.year = latest_year.yr
GROUP BY d.quarter_num, c.gender, c.age_group
ORDER BY d.quarter_num, c.gender, c.age_group;

-- Q5. Top Occupations by Product Category Sales
-- Highlights the top 5 occupations driving sales within each product category.

WITH occ_sales AS (
  SELECT p.product_category, c.occupation, SUM(s.sales_amount) AS total_revenue FROM walmartdw.sales s
  JOIN walmartdw.product p ON s.product_id = p.product_id
  JOIN walmartdw.customer c ON s.customer_id = c.customer_id
  GROUP BY p.product_category, c.occupation
)
SELECT product_category, occupation, total_revenue FROM (
  SELECT product_category, occupation, total_revenue,
    		ROW_NUMBER() OVER (PARTITION BY product_category ORDER BY total_revenue DESC) AS rn
  FROM occ_sales
) t
WHERE rn <= 5
ORDER BY product_category, rn;

-- Q6. City Category Performance by Marital Status with Monthly Breakdown
-- Assesses purchase amounts by city category and marital status over the past 6 months.
WITH max_date AS (
    SELECT MAX(transaction_date) AS latest_date FROM walmartdw.date
)
SELECT c.city_category, c.marital_status, d.year, d.monthNum,SUM(s.sales_amount) AS total_revenue, SUM(s.quantity) AS units_sold
FROM walmartdw.sales s
JOIN walmartdw.customer c ON s.customer_id = c.customer_id
JOIN walmartdw.date d ON s.date_id = d.date_id
JOIN max_date md ON d.transaction_date BETWEEN (md.latest_date - INTERVAL '6 months') AND md.latest_date
GROUP BY c.city_category, c.marital_status, d.year, d.monthNum
ORDER BY d.year, d.monthNum, c.city_category, c.marital_status;

-- Q7. Average Purchase Amount by Stay Duration and Gender
-- Calculates the average purchase amount based on years stayed in the city and gender.

SELECT c.stay_in_current_city_years, c.gender, AVG(s.sales_amount) AS avg_purchase_amount FROM walmartdw.sales s
JOIN walmartdw.customer c ON s.customer_id = c.customer_id
GROUP BY c.stay_in_current_city_years, c.gender
ORDER BY c.stay_in_current_city_years, c.gender;

-- Q8. Top 5 Revenue-Generating Cities by Product Category
-- Ranks the top 5 city categories by revenue, grouped by product category.

WITH city_rev AS (
    SELECT c.city_category, p.product_category, SUM(s.sales_amount) AS total_revenue
    FROM walmartdw.sales s
    JOIN walmartdw.customer c ON s.customer_id = c.customer_id
    JOIN walmartdw.product p ON s.product_id = p.product_id
    GROUP BY c.city_category, p.product_category
)
SELECT * FROM (
    SELECT city_category, product_category, total_revenue,
        ROW_NUMBER() OVER (
            PARTITION BY product_category
            ORDER BY total_revenue DESC
        ) AS rn FROM city_rev
) t WHERE rn <= 5
ORDER BY product_category, rn;

-- Q9. Monthly Sales Growth by Product Category
-- Measures month-over-month sales growth percentage for each product category in the current year.

WITH curr_year AS (
    SELECT MAX(year) AS yr FROM walmartdw.date
), monthly AS (
    SELECT p.product_category, d.monthNum, SUM(s.sales_amount) AS revenue
    FROM walmartdw.sales s
    JOIN walmartdw.date d ON s.date_id = d.date_id
    JOIN walmartdw.product p ON s.product_id = p.product_id
    JOIN curr_year cy ON d.year = cy.yr
    GROUP BY p.product_category, d.monthNum
)
SELECT product_category, monthNum, revenue, 
	LAG(revenue) OVER (PARTITION BY product_category ORDER BY monthNum) AS prev_revenue,
	    ROUND(
	        (revenue - LAG(revenue) 
				OVER (PARTITION BY product_category ORDER BY monthNum))
	        / NULLIF(LAG(revenue) 
				OVER (PARTITION BY product_category ORDER BY monthNum), 0) * 100, 2
	    ) AS growth_percent
FROM monthly
ORDER BY product_category, monthNum;

-- Q10. Weekend vs. Weekday Sales by Age Group
-- Compares total sales by age group for weekends versus weekdays in the current year.
WITH cy AS 
	(SELECT MAX(year) AS yr FROM walmartdw.date)
SELECT c.age_group, d.is_weekend, SUM(s.sales_amount) AS total_revenue FROM walmartdw.sales s
JOIN walmartdw.customer c ON s.customer_id = c.customer_id
JOIN walmartdw.date d ON s.date_id = d.date_id
JOIN cy ON d.year = cy.yr
GROUP BY c.age_group, d.is_weekend
ORDER BY c.age_group, d.is_weekend;

-- Q11. Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
-- Find the top 5 products that generated the highest revenue, separated by weekday and weekend sales, 4
-- with results grouped by month for a specified year.
WITH base AS (
    SELECT p.product_id, p.product_category, d.monthNum, d.is_weekend, SUM(s.sales_amount) AS revenue FROM walmartdw.sales s
    JOIN walmartdw.product p ON s.product_id = p.product_id
    JOIN walmartdw.date d ON s.date_id = d.date_id
    WHERE d.year = 2017   
    GROUP BY p.product_id, p.product_category, d.monthNum, d.is_weekend
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY monthNum, is_weekend
            ORDER BY revenue DESC
        ) AS rn
    FROM base
)
SELECT * FROM ranked WHERE rn <= 5
ORDER BY monthNum, is_weekend, revenue DESC;


-- Q12. Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
-- Calculate the revenue growth rate for each store on a quarterly basis for 2017.
WITH quarterly AS (
    SELECT s.store_id, d.quarter_num, SUM(s.sales_amount) AS revenue FROM walmartdw.sales s
    JOIN walmartdw.date d ON s.date_id = d.date_id
    WHERE d.year = 2017
    GROUP BY s.store_id, d.quarter_num
)
SELECT store_id, quarter_num, revenue, LAG(revenue) OVER (PARTITION BY store_id ORDER BY quarter_num) AS prev_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (PARTITION BY store_id ORDER BY quarter_num))/NULLIF(LAG(revenue) OVER (PARTITION BY store_id ORDER BY quarter_num), 0)
        * 100, 2
    ) AS growth_rate_percent
FROM quarterly
ORDER BY store_id, quarter_num;

-- Q13. Detailed Supplier Sales Contribution by Store and Product Name
-- For each store, show the total sales contribution of each supplier broken down by product name. 
-- The output should group results by store, then supplier, and then product name under each supplier.
SELECT st.storeName, sp.supplierName, p.product_category AS product_name, SUM(s.sales_amount) AS total_revenue FROM walmartdw.sales s
JOIN walmartdw.store st ON s.store_id = st.store_id
JOIN walmartdw.supplier sp ON s.supplier_id = sp.supplier_id
JOIN walmartdw.product p ON s.product_id = p.product_id
GROUP BY st.storeName, sp.supplierName, p.product_category
ORDER BY st.storeName, sp.supplierName, total_revenue DESC;

-- Q14. Seasonal Analysis of Product Sales Using Dynamic Drill-Down
-- Present total sales for each product, drilled down by seasonal periods (Spring, Summer, Fall, Winter). 
-- This can help understand product performance across seasonal periods.
SELECT p.product_id, p.product_category,
    CASE
        WHEN d.monthNum IN (3, 4, 5)   THEN 'Spring'
        WHEN d.monthNum IN (6, 7, 8)   THEN 'Summer'
        WHEN d.monthNum IN (9, 10, 11) THEN 'Fall'
        ELSE 'Winter'
    END AS season,
    SUM(s.sales_amount) AS total_revenue FROM walmartdw.sales s
JOIN walmartdw.product p ON s.product_id = p.product_id
JOIN walmartdw.date d ON s.date_id =d.date_id
GROUP BY p.product_id,p.product_category, season
ORDER BY p.product_id,season;

-- Q15. Store-Wise and Supplier-Wise Monthly Revenue Volatility
-- Calculate the month-to-month revenue volatility for each store and supplier pair. 
-- Volatility can be defned as the percentage change in revenue from one month to the next, helping identify stores
-- or suppliers with highly fluctuating sales.
WITH monthly AS (
    SELECT
        s.store_id,
        s.supplier_id,
        d.year,
        d.monthNum,
        SUM(s.sales_amount) AS revenue
    FROM walmartdw.sales s
    JOIN walmartdw.date d ON s.date_id = d.date_id
    GROUP BY s.store_id, s.supplier_id, d.year, d.monthNum
)
SELECT store_id, supplier_id, year, monthNum, revenue, 
	LAG(revenue) OVER (
        PARTITION BY store_id, supplier_id
        ORDER BY year, monthNum
    ) AS prev_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (
            PARTITION BY store_id, supplier_id
            ORDER BY year, monthNum
        ))
        / NULLIF(LAG(revenue) OVER (
            PARTITION BY store_id, supplier_id
            ORDER BY year, monthNum
        ), 0) * 100, 2
    ) AS volatility_percent
FROM monthly
ORDER BY store_id,supplier_id,year, monthNum;

-- Q16. Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)
-- Identify the top 5 products frequently bought together within a set of orders 
-- (i.e multiple products purchased in the same transaction). 
-- This product affinity analysis could inform potential product bundling strategies.
WITH pairs AS (
    SELECT s1.order_id, s1.product_id AS product_a, s2.product_id AS product_b FROM walmartdw.sales s1
    JOIN walmartdw.sales s2 ON s1.order_id = s2.order_id AND s1.product_id < s2.product_id
)
SELECT product_a, product_b, COUNT(*) AS times_bought_together FROM pairs
GROUP BY product_a, product_b
ORDER BY times_bought_together DESC
LIMIT 5;


-- Q17. Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
-- Use the ROLLUP operation to aggregate yearly revenue data by store, supplier, and product,
-- enabling a comprehensive overview from individual product-level details up to total revenue per store. 
-- This query should provide an overview of cumulative and hierarchical sales figures.

SELECT st.storeName,d.year, sp.supplierName, p.product_category,SUM(s.sales_amount) AS yearly_revenue FROM walmartdw.sales s
JOIN walmartdw.store st ON s.store_id = st.store_id
JOIN walmartdw.supplier sp ON s.supplier_id = sp.supplier_id
JOIN walmartdw.product p ON s.product_id = p.product_id
JOIN walmartdw.date d ON s.date_id = d.date_id
WHERE d.year = (SELECT MAX(year) FROM walmartdw.date)
GROUP BY ROLLUP (st.storeName, sp.supplierName, p.product_category, d.year)
ORDER BY st.storeName, sp.supplierName, p.product_category;

-- Q18. Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
-- For each product, calculate the total revenue and quantity sold in the first and second halves of
-- the year, along with yearly totals. This split-by-time-period analysis can reveal changes in
-- product popularity or demand over the year.
SELECT p.product_id, p.product_category,
    SUM(CASE WHEN d.monthNum BETWEEN 1 AND 6 THEN s.sales_amount END) AS h1_revenue,
    SUM(CASE WHEN d.monthNum BETWEEN 7 AND 12 THEN s.sales_amount END) AS h2_revenue,
    SUM(s.sales_amount) AS total_revenue,
    SUM(CASE WHEN d.monthNum BETWEEN 1 AND 6 THEN s.quantity END) AS h1_quantity,
    SUM(CASE WHEN d.monthNum BETWEEN 7 AND 12 THEN s.quantity END) AS h2_quantity,
    SUM(s.quantity) AS total_quantity
FROM walmartdw.sales s
JOIN walmartdw.product p ON s.product_id = p.product_id
JOIN walmartdw.date d ON s.date_id = d.date_id
WHERE d.year=(SELECT MAX(year) FROM walmartdw.date)
GROUP BY p.product_id, p.product_category
ORDER BY total_revenue DESC;

-- Q19. Identify High Revenue Spikes in Product Sales and Highlight Outliers
-- Calculate daily average sales for each product and flag days where the sales exceed twice the daily
-- average by product as potential outliers or spikes. Explain any identified anomalies in the report,
-- as these may indicate unusual demand events.
WITH daily_sales AS (
    SELECT p.product_id, d.transaction_date, SUM(s.sales_amount) AS daily_total
    FROM walmartdw.sales s
    JOIN walmartdw.product p ON s.product_id = p.product_id
    JOIN walmartdw.date d ON s.date_id = d.date_id
    GROUP BY p.product_id, d.transaction_date
),
product_avg AS (
    SELECT product_id, AVG(daily_total) AS avg_daily_sales FROM daily_sales
    GROUP BY product_id
)
SELECT ds.product_id, ds.transaction_date, ds.daily_total, pa.avg_daily_sales,
    CASE 
        WHEN ds.daily_total > 2 * pa.avg_daily_sales THEN 'SPIKE'
        ELSE 'NORMAL'
    END AS status
FROM daily_sales ds
JOIN product_avg pa ON ds.product_id = pa.product_id
WHERE ds.daily_total > 2 * pa.avg_daily_sales
ORDER BY ds.product_id, ds.transaction_date;

-- Q20. Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
-- Create a view named STORE_QUAsweRTERLY_SALES that aggregates total quarterly sales by store,
-- ordered by store name. This view allows quick retrieval of store-specific trends across quarters,
-- significantly improving query performance for regular sales analysis.
CREATE OR REPLACE VIEW walmartdw.store_quarterly_sales AS
SELECT st.store_id, st.storeName, d.year, d.quarter_num, SUM(s.sales_amount) AS total_quarterly_sales FROM walmartdw.sales s
JOIN walmartdw.store st ON s.store_id = st.store_id
JOIN walmartdw.date d ON s.date_id = d.date_id
GROUP BY st.store_id, st.storeName, d.year, d.quarter_num
ORDER BY st.storeName, d.year, d.quarter_num;

SELECT * FROM store_quarterly_sales;