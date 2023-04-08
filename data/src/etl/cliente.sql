WITH tb_join AS (
    SELECT
        DISTINCT
            t1.idCliente,
            t1.idPedido,
            t2.idVendedor,
            t3.descUF

    FROM pedido AS t1

    LEFT JOIN item_pedido as t2
    ON t1.idPedido = t2.idPedido

    LEFT JOIN cliente as t3
    ON t1.idCliente = t3.idCliente

    WHERE t1.dtPedido < '2018-01-01'
    AND t1.dtPedido >= DATE('2018-01-01', '-6 MONTH')
    AND t2.idVendedor IS NOT NULL
)

SELECT *
FROM tb_join