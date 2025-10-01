SELECT 
    
    sales.products_id,
    SUM(quantity) AS quantity,
    SUM(CAST(product.purchase_price AS FLOAT64)) AS purchase_price,
    SUM(quantity * CAST(product.purchase_price AS FLOAT64)) AS purchase_cost,
    SUM(revenue) - SUM(quantity * CAST(product.purchase_price AS FLOAT64)) AS margin
   
FROM {{ ref('stg_raw__sales') }} AS sales
JOIN {{ ref('stg_raw__product') }} AS product
    ON sales.products_id = product.products_id
GROUP BY sales.products_id