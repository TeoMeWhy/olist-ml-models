WITH tb_pedido AS (

  SELECT t1.idPedido,
        t2.idVendedor,
        t1.descSituacao,
        t1.dtPedido,
        t1.dtAprovado,
        t1.dtEntregue,
        t1.dtEstimativaEntrega,
        sum(vlFrete) as totalFrente       

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido as t2
  ON t1.idPedido = t2.idPedido

  WHERE dtPedido < '{date}'
  AND dtPedido >= add_months('{date}', -6)

  GROUP BY t1.idPedido,
          t2.idVendedor,
          t1.descSituacao,
          t1.dtPedido,
          t1.dtAprovado,
          t1.dtEntregue,
          t1.dtEstimativaEntrega
  )

SELECT

    '{date}' AS dtReference,
    NOW() AS dtIngestion,
    idVendedor,
    COUNT(DISTINCT CASE WHEN date(coalesce(dtEntregue, '{date}')) > date(dtEstimativaEntrega) THEN idPedido END) / COUNT(DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS pctPedidoAtraso,
    count(distinct case when descSituacao = 'canceled' then idPedido end) / count(distinct idPedido) AS pctPedidoCancelado,
    avg(totalFrente) as avgFrete,
    percentile(totalFrente, 0.5) as medianFrete,
    max(totalFrente) as maxFrete,
    min(totalFrente) as minFrete,
    avg(datediff(coalesce(dtEntregue, '{date}'), dtAprovado)) AS qtdDiasAprovadoEntrega,
    avg(datediff(coalesce(dtEntregue, '{date}'), dtPedido)) AS qtdDiasPedidoEntrega,
    avg(datediff(dtEstimativaEntrega, coalesce(dtEntregue, '{date}'))) AS qtdeDiasEntregaPromessa
      
FROM tb_pedido

GROUP BY 1,2,3