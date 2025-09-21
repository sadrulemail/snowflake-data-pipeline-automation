-- Sales Performance by Manufacturer and Country
SELECT
    m.country,
    YEAR(s.sale_date) AS sales_year,
    SUM(s.total_amount) AS total_sales
FROM
    TRANSFORM_DATA.FACT_SALES AS s
JOIN
    TRANSFORM_DATA.FACT_SALES_LINE_ITEMS AS sli
    ON s.sale_id = sli.sale_id
JOIN
    TRANSFORM_DATA.DIM_PRODUCTS AS p
    ON sli.product_key = p.product_key AND p.is_current = TRUE
JOIN
    TRANSFORM_DATA.DIM_MANUFACTURERS AS m
    ON p.manufacturer_id = m.manufacturer_id AND m.is_current = TRUE
GROUP BY
    m.country,
    sales_year
ORDER BY
    sales_year,
    total_sales DESC;

--Products with Low Inventory Levels

SELECT
    p.product_name,
    m.manufacturer_name,
    i.quantity_on_hand,
    i.low_stock_threshold,
    i.warehouse_location
FROM
    TRANSFORM_DATA.DIM_INVENTORY AS i
JOIN
    TRANSFORM_DATA.DIM_PRODUCTS AS p
    ON i.product_id = p.product_id AND p.is_current = TRUE
JOIN
    TRANSFORM_DATA.DIM_MANUFACTURERS AS m
    ON p.manufacturer_id = m.manufacturer_id AND m.is_current = TRUE
WHERE
    i.quantity_on_hand < i.low_stock_threshold
    AND i.is_current = TRUE
ORDER BY
    i.quantity_on_hand;


--Monthly Sales and Unit Count with a 3-Month Moving Average	
SELECT
    DATE_TRUNC('month', s.sale_date) AS sales_month,
    SUM(sli.quantity) AS total_units_sold,
    SUM(s.total_amount) AS total_monthly_sales,
    AVG(total_monthly_sales) OVER (
        ORDER BY DATE_TRUNC('month', s.sale_date) 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS three_month_moving_avg
FROM
    TRANSFORM_DATA.FACT_SALES AS s
JOIN
    TRANSFORM_DATA.FACT_SALES_LINE_ITEMS AS sli
    ON s.sale_id = sli.sale_id
GROUP BY
    sales_month
ORDER BY
    sales_month;
	
--Top 5 Customers by Total Lifetime Spending
	
SELECT
    c.customer_name,
    c.city,
    SUM(s.total_amount) AS total_spending
FROM
    TRANSFORM_DATA.FACT_SALES AS s
JOIN
    TRANSFORM_DATA.DIM_CUSTOMERS AS c
    ON s.customer_key = c.customer_key AND c.is_current = TRUE
GROUP BY
    c.customer_name,
    c.city
ORDER BY
    total_spending DESC
LIMIT 5;

-- Total Sales by Product Category and Year

SELECT
    p.category,
    YEAR(s.sale_date) AS sales_year,
    SUM(sli.line_total_amount) AS total_revenue
FROM
    TRANSFORM_DATA.FACT_SALES_LINE_ITEMS AS sli
JOIN
    TRANSFORM_DATA.DIM_PRODUCTS AS p
    ON sli.product_key = p.product_key AND p.is_current = TRUE
JOIN
    TRANSFORM_DATA.FACT_SALES AS s
    ON sli.sale_id = s.sale_id
GROUP BY
    p.category,
    sales_year
ORDER BY
    sales_year,
    total_revenue DESC;