-- Databricks notebook source
-- DROP TABLE IF EXISTS sandbox.analystics_churn_model.fs_vendedor_avaliacao; 
-- CREATE TABLE sandbox.analystics_churn_model.fs_vendedor_avaliacao AS 

WITH tb_pedido AS (

  SELECT DISTINCT 
    t1.idPedido,
    t2.idVendedor
  FROM 
    silver.olist.pedido AS t1
  LEFT JOIN 
    silver.olist.item_pedido as t2
      ON t1.idPedido = t2.idPedido
  WHERE 
    t1.dtPedido < '2018-01-01'
  AND 
    t1.dtPedido >= ADD_MONTHS('2018-01-01', -6)
  AND 
    idVendedor IS NOT NULL), 


tbl_avalicao AS (
SELECT 
  t1.*, 
  t2.idAvaliacao, 
  t2.vlNota 
FROM 
  tb_pedido t1
LEFT JOIN 
  silver.olist.avaliacao_pedido t2 
ON 
  t1.idPedido = t2.idPedido) 


SELECT 
  '2018-01-01' AS DateRef,
  idVendedor, 
  AVG(COALESCE(vlNota,0)) AS AvgNota, 
  percentile(vlNota, 0.5) AS MedNota, 
  MIN(vlNota) AS MinNota, 
  MAX(vlNota) MaxNota, 
  COUNT(DISTINCT idAvaliacao) AS NumAvaliacao, 
  COUNT(vlNota) / COUNT (idPedido) AS perc_Avalicao, 
  COUNT(DISTINCT CASE WHEN COALESCE(vlNota,0)  < 3  THEN idAvaliacao END) AS NumAvaliacaoNegativa, 
  COUNT(DISTINCT CASE WHEN COALESCE(vlNota,0)  = 3 THEN idAvaliacao END) AS NumAvaliacaoNeutra, 
  COUNT(DISTINCT CASE WHEN COALESCE(vlNota,0)  > 3 THEN idAvaliacao END) AS NumAvaliacaoPositiva 
FROM 
  tbl_avalicao
GROUP BY 
  idVendedor; 






-- COMMAND ----------

select * from analytics.asn.fs_seller_avaliacao 

-- COMMAND ----------

-- COMMAND ----------

-- WITH tb_pedido AS (

--   SELECT DISTINCT 
--     t1.idPedido,
--     t2.idVendedor
--   FROM 
--     silver.olist.pedido AS t1
--   LEFT JOIN 
--     silver.olist.item_pedido as t2
--       ON t1.idPedido = t2.idPedido
--   WHERE 
--     t1.dtPedido < '2018-01-01'
--   AND 
--     t1.dtPedido >= ADD_MONTHS('2018-01-01', -6)
--   AND 
--     idVendedor IS NOT NULL), 


-- tbl_avalicao AS (
-- SELECT 
--   t1.*, 
--   t2.idAvaliacao, 
--   t2.vlNota 
-- FROM 
--   tb_pedido t1
-- LEFT JOIN 
--   silver.olist.avaliacao_pedido t2 
-- ON 
--   t1.idPedido = t2.idPedido) 


-- SELECT 
--   idVendedor, 
--   AVG(COALESCE(vlNota,0)) AS AvgNota, 
--   percentile(vlNota, 0.5) AS MedNota, 
--   MIN(vlNota) AS MinNota, 
--   MAX(vlNota) MaxNota, 
--   COUNT(DISTINCT idAvaliacao) AS NumAvaliacao, 
--   COUNT(vlNota) / COUNT (idPedido) AS perc_Avalicao, 
--   COUNT(DISTINCT CASE WHEN COALESCE(vlNota,0)  < 3  THEN idAvaliacao END) AS NumAvaliacaoNegativa, 
--   COUNT(DISTINCT CASE WHEN COALESCE(vlNota,0) = 3 THEN idAvaliacao END) AS NumAvaliacaoNeutra, 
--   COUNT(DISTINCT CASE WHEN COALESCE(vlNota,0)  > 3 THEN idAvaliacao END) AS NumAvaliacaoPositiva 
-- FROM 
--   tbl_avalicao
-- where 
--   idVendedor = '0ea22c1cfbdc755f86b9b54b39c16043'
-- GROUP BY 
--   idVendedor 
