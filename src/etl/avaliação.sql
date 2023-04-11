WITH tb_pedido AS (

SELECT DISTINCT 
       t1.idPedido,
       t2.idVendedor 
FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido


WHERE dtPedido < '{date}'
AND dtPedido >= add_months('{date}', -6)
AND idVendedor IS NOT NULL),

tb_join AS (

SELECT t1.*,
       t2.vlNota

FROM tb_pedido AS t1

LEFT JOIN silver.olist.avaliacao_pedido AS t2
ON t1.idPedido = t2.idPedido),

tb_summary AS (

SELECT idVendedor,
       AVG(vlNota) as avgNota,
       percentile(vlNota, 0.5) as medianNota,
       min(vlNota) as minNota,
       max(vlNota) as maxNota,
       count(vlNota) / count(idPedido) as pctAvaliacao

FROM tb_join

GROUP BY 1)

SELECT '{date}' dtReference,
        now() as dtIngestion,
        *
        

FROM tb_summary