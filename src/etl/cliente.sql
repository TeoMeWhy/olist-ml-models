-- Databricks notebook source
WITH tb_join AS (

  SELECT DISTINCT
         t1.idPedido,
         t1.idCliente,
         t2.idVendedor,
         t3.descUF

  FROM silver.olist.pedido AS  t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.cliente AS t3
  ON t1.idCliente = t3.idCliente

  WHERE dtPedido < '2018-01-01'
  AND dtPedido >= add_months('2018-01-01', -6)
  AND idVendedor IS NOT NULL

),

tb_group AS (

  SELECT
    idVendedor,

    count(DISTINCT descUF) as qtdUFsPedidos,

    count(DISTINCT CASE WHEN descUF = 'AC' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoAC,
    count(DISTINCT CASE WHEN descUF = 'AL' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoAL,
    count(DISTINCT CASE WHEN descUF = 'AM' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoAM,
    count(DISTINCT CASE WHEN descUF = 'AP' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoAP,
    count(DISTINCT CASE WHEN descUF = 'BA' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoBA,
    count(DISTINCT CASE WHEN descUF = 'CE' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoCE,
    count(DISTINCT CASE WHEN descUF = 'DF' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoDF,
    count(DISTINCT CASE WHEN descUF = 'ES' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoES,
    count(DISTINCT CASE WHEN descUF = 'GO' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoGO,
    count(DISTINCT CASE WHEN descUF = 'MA' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoMA,
    count(DISTINCT CASE WHEN descUF = 'MG' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoMG,
    count(DISTINCT CASE WHEN descUF = 'MS' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoMS,
    count(DISTINCT CASE WHEN descUF = 'MT' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoMT,
    count(DISTINCT CASE WHEN descUF = 'PA' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoPA,
    count(DISTINCT CASE WHEN descUF = 'PB' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoPB,
    count(DISTINCT CASE WHEN descUF = 'PE' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoPE,
    count(DISTINCT CASE WHEN descUF = 'PI' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoPI,
    count(DISTINCT CASE WHEN descUF = 'PR' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoPR,
    count(DISTINCT CASE WHEN descUF = 'RJ' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoRJ,
    count(DISTINCT CASE WHEN descUF = 'RN' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoRN,
    count(DISTINCT CASE WHEN descUF = 'RO' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoRO,
    count(DISTINCT CASE WHEN descUF = 'RR' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoRR,
    count(DISTINCT CASE WHEN descUF = 'RS' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoRS,
    count(DISTINCT CASE WHEN descUF = 'SC' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoSC,
    count(DISTINCT CASE WHEN descUF = 'SE' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoSE,
    count(DISTINCT CASE WHEN descUF = 'SP' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoSP,
    count(DISTINCT CASE WHEN descUF = 'TO' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoTO

  FROM tb_join

  GROUP BY idVendedor

)

SELECT 
    '2018-01-01' AS dtReference,
    *

FROM tb_group

-- COMMAND ----------


