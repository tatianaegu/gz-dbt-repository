SELECT 
    sales.products_id,
    date_date,
    SUM (revenue) AS revenue,
    SUM(quantity) AS quantity,

    SUM(revenue - CAST(product.purchase_price AS FLOAT64)) AS margin,
    SUM(quantity * CAST(product.purchase_price AS FLOAT64)) AS purchase_cost
FROM {{ ref('stg_raw__sales') }} AS sales
JOIN {{ ref('stg_raw__product') }} AS product
    ON sales.products_id = product.products_id
GROUP BY date_date, products_id