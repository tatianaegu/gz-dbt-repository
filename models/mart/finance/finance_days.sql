WITH sales_margin AS (
    SELECT 
        date_date,
        COUNT(DISTINCT orders_id) AS number_of_transactions,
        SUM(quantity) AS quantity,
        SUM(CAST(product.purchase_price AS FLOAT64)) AS purchase_price,
        SUM(revenue - (quantity * CAST(product.purchase_price AS FLOAT64))) AS margin,
        SUM(quantity * CAST(product.purchase_price AS FLOAT64)) AS purchase_cost,
        SUM(revenue) AS revenue
    FROM {{ ref('stg_raw__sales') }} AS sales
    JOIN {{ ref('stg_raw__product') }} AS product
        ON sales.products_id = product.products_id
    GROUP BY date_date
),

ship_daily AS (
    SELECT
        sales.date_date,
        SUM(ship.shipping_fee) AS total_shipping_fee,
        SUM(ship.logcost) AS total_logcost,
        SUM(ship.ship_cost) AS total_ship_cost
    FROM {{ ref('stg_raw__sales') }} AS sales
    JOIN {{ ref('stg_raw__ship') }} AS ship
        ON sales.orders_id = ship.orders_id
    GROUP BY sales.date_date
)

SELECT
    sales_margin.date_date,
    sales_margin.number_of_transactions,
    sales_margin.quantity,
    sales_margin.revenue,
    (sales_margin.revenue / sales_margin.number_of_transactions) AS average_basket,
    sales_margin.purchase_price,
    sales_margin.margin,
    sales_margin.purchase_cost,
    ship_daily.total_shipping_fee,
    ship_daily.total_ship_cost,
    ship_daily.total_logcost,
    (sales_margin.margin + ship_daily.total_shipping_fee - ship_daily.total_logcost - ship_daily.total_ship_cost) AS operational_margin
FROM sales_margin
JOIN ship_daily
    ON sales_margin.date_date = ship_daily.date_date