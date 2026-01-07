select o.customer_id, c.name, c.country, p.name, p.category, o.quantity, o.total_amount,o.unit_price, p.price
from `advance-sql-de-demo.bronze.orders` as o
join `advance-sql-de-demo.bronze.products` as p on o.product_id = p.id
join `advance-sql-de-demo.bronze.customers` as c on o.customer_id = c.id