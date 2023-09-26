-- Databricks notebook source
WITH base_geral AS ( 
SELECT DISTINCT a.idPedido,
a.idCliente,
b.idVendedor,
c.descUF

FROM silver.olist.pedido AS a

LEFT JOIN silver.olist.item_pedido AS b
    ON a.idPedido = b.idPedido

LEFT JOIN silver.olist.cliente AS c
    ON a.idCliente = c.idCliente

WHERE dtPedido < '2018-01-01'
  AND dtPedido >= add_months('2018-01-01', -6 )
  AND idVendedor IS NOT NULL),


base_resumo AS (
SELECT idVendedor,
    COUNT(DISTINCT descUF) AS qnt_UFs_atendidos,
    COUNT(CASE WHEN descUF = 'AC' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_AC ,
    COUNT(CASE WHEN descUF = 'AL' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_AL ,
    COUNT(CASE WHEN descUF = 'AM' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_AM ,
    COUNT(CASE WHEN descUF = 'AP' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_AP ,
    COUNT(CASE WHEN descUF = 'BA' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_BA ,
    COUNT(CASE WHEN descUF = 'CE' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_CE ,
    COUNT(CASE WHEN descUF = 'DF' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_DF ,
    COUNT(CASE WHEN descUF = 'ES' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_ES ,
    COUNT(CASE WHEN descUF = 'GO' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_GO ,
    COUNT(CASE WHEN descUF = 'MA' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_MA ,
    COUNT(CASE WHEN descUF = 'MG' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_MG ,
    COUNT(CASE WHEN descUF = 'MS' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_MS ,
    COUNT(CASE WHEN descUF = 'MT' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_MT ,
    COUNT(CASE WHEN descUF = 'PA' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_PA ,
    COUNT(CASE WHEN descUF = 'PB' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_PB ,
    COUNT(CASE WHEN descUF = 'PE' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_PE ,
    COUNT(CASE WHEN descUF = 'PI' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_PI ,
    COUNT(CASE WHEN descUF = 'PR' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_PR ,
    COUNT(CASE WHEN descUF = 'RJ' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_RJ ,
    COUNT(CASE WHEN descUF = 'RN' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_RN ,
    COUNT(CASE WHEN descUF = 'RO' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_RO ,
    COUNT(CASE WHEN descUF = 'RR' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_RR ,
    COUNT(CASE WHEN descUF = 'RS' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_RS ,
    COUNT(CASE WHEN descUF = 'SC' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_SC ,
    COUNT(CASE WHEN descUF = 'SE' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_SE ,
    COUNT(CASE WHEN descUF = 'SP' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_SP ,
    COUNT(CASE WHEN descUF = 'TO' THEN idPedido end) / COUNT(DISTINCT idPedido) AS perc_pedidos_TO 

FROM base_geral

GROUP BY idVendedor)

SELECT *,
    '2018-01-01' AS data_referencia

FROM base_resumo





