WITH tb_pedido_item AS (
    SELECT *
    FROM pedido AS t1
    LEFT JOIN item_pedido AS t2
        ON t1.idPedido = t2.idPedido
    WHERE t1.dtPedido < '2018-01-01'
    AND t1.dtPedido >= DATE('2018-01-01','-6 MONTH')
    AND t2.idVendedor IS NOT NULL
),

tb_summary AS (
    SELECT 
        idVendedor,
        COUNT(DISTINCT idPedido) AS qtdPedidos,
        COUNT(DISTINCT DATE(dtPedido)) AS qtdDias,
        COUNT(idProduto) AS qtdItens,
        MIN(JULIANDAY('2018-01-01') - JULIANDAY(dtPedido)) AS qtdRecencia,
        SUM(vlPreco) / SUM(DISTINCT idPedido) AS avgTicket,
        AVG(vlPreco) AS avgValorProduto,
        MAX(vlPreco) AS maxValorProduto,
        MIN(vlPreco) AS minValorProduto,
        COUNT(idProduto) / COUNT(DISTINCT idPedido) as avgProdutoPedido
    FROM tb_pedido_item
    GROUP BY 1
),

tb_pedido_summary AS (
    SELECT 
        idVendedor,
        idPedido,
        sum(vlPreco) as vlPreco
    FROM tb_pedido_item
    GROUP BY 1, 2
),

tb_min_max AS (
    SELECT 
        idVendedor,
        MIN(vlPreco) AS minVlPedido,
        MAX(vlPreco) AS maxVlPedido
    FROM tb_pedido_summary
    GROUP BY 1
),

tb_life AS (
    SELECT 
        t2.idVendedor,
        SUM(vlPreco) AS LTV,
        MAX(julianday('2018-01-01') - julianday(dtPedido)) as qtdeDiasBase
    FROM pedido AS t1
    LEFT JOIN item_pedido AS t2
        ON t1.idPedido = t2.idPedido
    WHERE t1.dtPedido < '2018-01-01'
    AND t1.dtPedido >= DATE('2018-01-01','-6 MONTH')
    AND t2.idVendedor IS NOT NULL
    GROUP BY 1
),

tb_dtpedido AS (
    SELECT 
        DISTINCT idVendedor,
        DATE(dtPedido) AS dtPedido
    FROM tb_pedido_item
    ORDER BY 1, 2
),

tb_lag_dtpedido AS (
    SELECT 
        *,
        LAG(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido) AS lag1
    FROM tb_dtpedido
),

tb_intervalo AS (
    SELECT 
        idVendedor,
        AVG(JULIANDAY(dtPedido) - JULIANDAY(lag1)) AS avgIntervaloVendas
    FROM tb_lag_dtpedido
    GROUP BY 1
)

SELECT 
    '2018-01-01' AS dtReference,
    t1.*,
    t2.minVlPedido,
    t2.maxVlPedido,
    t3.LTV,
    t3.qtdeDiasBase,
    t4.avgIntervaloVendas
FROM tb_summary AS t1
LEFT JOIN tb_min_max AS t2
    ON t1.idVendedor = t2.idVendedor
LEFT JOIN tb_life AS t3
    ON t1.idVendedor = t3.idVendedor
LEFT JOIN tb_intervalo AS t4
    ON t1.idVendedor = t4.idVendedor