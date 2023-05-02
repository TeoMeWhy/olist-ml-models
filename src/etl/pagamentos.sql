SELECT DATE(dtPedido) as dtPedido,
    count(*) as qtPedido

FROM pedido
GROUP BY 1
ORDER BY 1