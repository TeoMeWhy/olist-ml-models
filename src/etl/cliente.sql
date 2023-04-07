-- Databricks notebook source
WITH 
tb_join AS (
  SELECT
    t1.idPedido,
    t1.idCliente,
    t2.idVendedor,
    t3.descUF

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.cliente AS t3
  ON t1.idCliente = t3.idCliente

  WHERE 
    dtPedido BETWEEN '2017-07-01' AND '2018-01-01'
    AND idVendedor IS NOT NULL
),

tb_grouped AS(
  SELECT
    idVendedor,
    COUNT(DISTINCT descUF) AS qtdUFsPedidos, -- Total de estados que houve vendas por vendedor
    -- Proporção de pedidos por estado e vendedor
    COUNT(DISTINCT CASE WHEN descUF = 'AC' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoAC,
    COUNT(DISTINCT CASE WHEN descUF = 'AL' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoAL,
    COUNT(DISTINCT CASE WHEN descUF = 'AM' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoAM,
    COUNT(DISTINCT CASE WHEN descUF = 'AP' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoAP,
    COUNT(DISTINCT CASE WHEN descUF = 'BA' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoBA,
    COUNT(DISTINCT CASE WHEN descUF = 'CE' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoCE,
    COUNT(DISTINCT CASE WHEN descUF = 'DF' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoDF,
    COUNT(DISTINCT CASE WHEN descUF = 'ES' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoES,
    COUNT(DISTINCT CASE WHEN descUF = 'GO' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoGO,
    COUNT(DISTINCT CASE WHEN descUF = 'MA' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoMA,
    COUNT(DISTINCT CASE WHEN descUF = 'MG' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoMG,
    COUNT(DISTINCT CASE WHEN descUF = 'MS' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoMS,
    COUNT(DISTINCT CASE WHEN descUF = 'MT' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoMT,
    COUNT(DISTINCT CASE WHEN descUF = 'PA' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoPA,
    COUNT(DISTINCT CASE WHEN descUF = 'PB' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoPB,
    COUNT(DISTINCT CASE WHEN descUF = 'PE' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoPE,
    COUNT(DISTINCT CASE WHEN descUF = 'PI' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoPI,
    COUNT(DISTINCT CASE WHEN descUF = 'PR' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoPR,
    COUNT(DISTINCT CASE WHEN descUF = 'RJ' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoRJ,
    COUNT(DISTINCT CASE WHEN descUF = 'RN' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoRN,
    COUNT(DISTINCT CASE WHEN descUF = 'RO' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoRO,
    COUNT(DISTINCT CASE WHEN descUF = 'RR' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoRR,
    COUNT(DISTINCT CASE WHEN descUF = 'RS' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoRS,
    COUNT(DISTINCT CASE WHEN descUF = 'SC' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoSC,
    COUNT(DISTINCT CASE WHEN descUF = 'SE' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoSE,
    COUNT(DISTINCT CASE WHEN descUF = 'SP' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoSP,
    COUNT(DISTINCT CASE WHEN descUF = 'TO' THEN idPedido END) / COUNT(DISTINCT idPedido) AS pctPedidoTO

  FROM tb_join
  GROUP BY idvendedor
)

SELECT 
  '2018-01-01' AS dtReference,
  * 
FROM tb_grouped
