-- Databricks notebook source
WITH tb_join AS (
SELECT 
  DISTINCT
    t1.idPedido,
    t1.idCliente,
    t2.idVendedor,
    t3.descUF

FROM silver.olist.pedido AS t1

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
  COUNT(DISTINCT descUF) AS qtdUFsPedidos,
  COUNT(DISTINCT CASE WHEN descUF = 'SC' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteSC,
  COUNT(DISTINCT CASE WHEN descUF = 'RO' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteRO,
  COUNT(DISTINCT CASE WHEN descUF = 'PI' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClientePI,
  COUNT(DISTINCT CASE WHEN descUF = 'AM' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteAM,
  COUNT(DISTINCT CASE WHEN descUF = 'RR' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteRR,
  COUNT(DISTINCT CASE WHEN descUF = 'GO' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteGO,
  COUNT(DISTINCT CASE WHEN descUF = 'TO' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteTO,
  COUNT(DISTINCT CASE WHEN descUF = 'MT' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteMT,
  COUNT(DISTINCT CASE WHEN descUF = 'SP' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteSP,
  COUNT(DISTINCT CASE WHEN descUF = 'ES' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteES,
  COUNT(DISTINCT CASE WHEN descUF = 'PB' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClientePB,
  COUNT(DISTINCT CASE WHEN descUF = 'RS' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteRS,
  COUNT(DISTINCT CASE WHEN descUF = 'MS' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteMS,
  COUNT(DISTINCT CASE WHEN descUF = 'AL' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteAL,
  COUNT(DISTINCT CASE WHEN descUF = 'MG' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteMG,
  COUNT(DISTINCT CASE WHEN descUF = 'PA' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClientePA,
  COUNT(DISTINCT CASE WHEN descUF = 'BA' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteBA,
  COUNT(DISTINCT CASE WHEN descUF = 'SE' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteSE,
  COUNT(DISTINCT CASE WHEN descUF = 'PE' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClientePE,
  COUNT(DISTINCT CASE WHEN descUF = 'CE' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteCE,
  COUNT(DISTINCT CASE WHEN descUF = 'RN' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteRN,
  COUNT(DISTINCT CASE WHEN descUF = 'RJ' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteRJ,
  COUNT(DISTINCT CASE WHEN descUF = 'MA' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteMA,
  COUNT(DISTINCT CASE WHEN descUF = 'AC' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteAC,
  COUNT(DISTINCT CASE WHEN descUF = 'DF' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteDF,
  COUNT(DISTINCT CASE WHEN descUF = 'PR' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClientePR,
  COUNT(DISTINCT CASE WHEN descUF = 'AP' THEN idPedido END)/COUNT(DISTINCT idPedido) AS pctPedidoClienteAP

FROM tb_join

GROUP BY idVendedor
)

SELECT
  '2018-01-01' AS dtReference,
  *
FROM tb_group

-- COMMAND ----------


