source("r/sparklabs/batch/sales/sql/utils.R")

showDF(run_sql("
  WITH revenue_per_order AS (
    SELECT oi.order_id,
           SUM(p.price * oi.quantity) AS total_revenue,
           COUNT(*) AS total_orders
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    GROUP BY oi.order_id
  )
  SELECT o.store_id,
         SUM(r.total_revenue) AS store_revenue,
         COUNT(*) AS total_store_orders
  FROM orders o
  JOIN revenue_per_order r ON o.order_id = r.order_id
  JOIN stores s ON o.store_id = s.store_id
  GROUP BY o.store_id
  ORDER BY store_revenue DESC
"), truncate = FALSE)
