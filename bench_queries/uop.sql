
SELECT
    users.uid,
    orders.oid,
    products.pid
FROM
    users, products, orders
WHERE
        users.uid > orders.uid
    and products.pid = orders.pid
    and products.name = 'Toilet'
