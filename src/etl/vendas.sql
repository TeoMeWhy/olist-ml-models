-- CREATE TABLE sandbox.analystics_churn_model.fs_vendedor_vendas AS

WITH tbl_pedido AS
(
SELECT 
t2.*, t1.dtPedido
FROM 
  silver.olist.pedido t1 
LEFT JOIN 
  silver.olist.item_pedido t2 
    ON t1.idPedido = t2.idPedido 
WHERE 
  dtPedido >= ADD_MONTHS('{date}', -6)
AND 
  dtPedido < '{date}'
AND 
  idVendedor IS NOT NULL
), 

tbl_summary AS 
(SELECT 
  idVendedor, 
  COUNT(DISTINCT idPedido) AS QntPedidos, 
  COUNT(DISTINCT DATE(dtPedido)) AS QntDias, 
  COUNT(DISTINCT idProduto) AS QntItens, 
  DATEDIFF('{date}', MAX(dtPedido)) AS QntRecencia, 
  SUM(vlPreco)/ COUNT(DISTINCT idPedido) AS AvgTicket, 
  AVG(vlPreco) AS AvgValorProduto, 
  MAX(vlPreco) AS MAXValorProduto, 
  MIN(vlPreco) AS MINValorProduto, 
  COUNT(idProduto) / COUNT(DISTINCT idPedido) AS AvgProdutoPedido 
FROM 
  tbl_pedido
GROUP BY 
  idVendedor), 


tbl_min_max AS (
SELECT 
  idVendedor, 
  MAX(vlPreco) AS MAXValorPedido, 
  MIN(vlPreco) AS MINValorPedido
FROM 
  (
  SELECT 
    idVendedor, 
    idPedido, 
    SUM(vlPreco) AS vlPreco 
  FROM 
    tbl_pedido 
  GROUP BY 
    idVendedor, 
    idPedido
  ) 
GROUP BY 
  idVendedor), 

tbl_life AS (
SELECT 
    t2.idVendedor, 
    SUM(vlPreco) AS LTV,
    MAX(DATEDIFF('{date}', dtPedido)) AS qtnDiasBase 
FROM 
  silver.olist.pedido t1 
LEFT JOIN 
  silver.olist.item_pedido t2 
    ON t1.idPedido = t2.idPedido 
WHERE 
  dtPedido < '{date}'
AND 
  idVendedor IS NOT NULL
GROUP BY 
  t2.idVendedor), 

tbl_intervalo AS (
SELECT 
  idVendedor, 
  AVG(DATEDIFF(dtPedido, NextDatePedido)) AS AvgIntervaloVendas
FROM 
(SELECT DISTINCT 
  idVendedor, 
  DATE(dtPedido) AS dtPedido,
  LAG(DATE(dtPedido),1) OVER (PARTITION BY IdVendedor ORDER BY dtPedido) AS NextDatePedido
FROM 
  tbl_pedido)
GROUP BY idVendedor) 


SELECT 
  '{date}' DateRef, 
  t1.*, 
  t2.MAXValorPedido,  
  t2.MINValorPedido, 
  t3.LTV, 
  t3.qtnDiasBase, 
  t4.AvgIntervaloVendas
FROM 
  tbl_summary t1 
LEFT JOIN 
  tbl_min_max t2 
    ON t1.idVendedor = t2.idVendedor 
LEFT JOIN 
  tbl_life t3
    ON t1.idVendedor = t3.idVendedor 
LEFT JOIN 
  tbl_intervalo t4
    ON t1.idVendedor = t4.idVendedor 

 


