-- Databricks notebook source
WITH base_geral AS ( 
SELECT DISTINCT
  b.idVendedor, 
  c.*

FROM silver.olist.pedido AS a

LEFT JOIN silver.olist.item_pedido AS b
  ON a.idPedido = b.idPedido

LEFT JOIN silver.olist.produto AS c
  ON b.idProduto = c.idProduto

WHERE a.dtPedido < '2018-01-01'
  AND a.dtPedido >= add_months('2018-01-01', -6)
  AND b.idVendedor IS NOT NULL),


base_resumo AS (
SELECT idVendedor,
  AVG(coalesce(nrFotos, 0)) AS media_fotos,
  AVG(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS media_volume,
  PERCENTILE(vlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) AS mediana_volume,
  MIN(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS min_volume,
  MAX(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS max_volume,

  
  COUNT(DISTINCT CASE WHEN descCategoria = 'cama_mesa_banho' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_cama_mesa_banho,
  COUNT(DISTINCT CASE WHEN descCategoria = 'beleza_saude' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_beleza_saude,
  COUNT(DISTINCT CASE WHEN descCategoria = 'esporte_lazer' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_esporte_lazer,
  COUNT(DISTINCT CASE WHEN descCategoria = 'informatica_acessorios' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_informatica_acessorios,
  COUNT(DISTINCT CASE WHEN descCategoria = 'moveis_decoracao' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_moveis_decoracao,
  COUNT(DISTINCT CASE WHEN descCategoria = 'utilidades_domesticas' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_utilidades_domesticas,
  COUNT(DISTINCT CASE WHEN descCategoria = 'relogios_presentes' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_relogios_presentes,
  COUNT(DISTINCT CASE WHEN descCategoria = 'telefonia' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_telefonia,
  COUNT(DISTINCT CASE WHEN descCategoria = 'automotivo' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_automotivo,
  COUNT(DISTINCT CASE WHEN descCategoria = 'brinquedos' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_brinquedos,
  COUNT(DISTINCT CASE WHEN descCategoria = 'cool_stuff' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_cool_stuff,
  COUNT(DISTINCT CASE WHEN descCategoria = 'ferramentas_jardim' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_ferramentas_jardim,
  COUNT(DISTINCT CASE WHEN descCategoria = 'perfumaria' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_perfumaria,
  COUNT(DISTINCT CASE WHEN descCategoria = 'bebes' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_bebes,
  COUNT(DISTINCT CASE WHEN descCategoria = 'eletronicos' THEN idProduto END) / COUNT(DISTINCT idProduto) AS perc_categoria_eletronicos
  
FROM base_geral 

GROUP BY idVendedor)

SELECT *,
  '2018-01-01' AS data_referencia
  
FROM base_resumo










-- COMMAND ----------

SELECT descCategoria

FROM silver.olist.item_pedido AS b

LEFT JOIN silver.olist.produto AS c
  ON b.idProduto = c.idProduto

WHERE b.idVendedor IS NOT NULL

GROUP BY ALL
ORDER BY COUNT(DISTINCT idPedido) DESC

LIMIT 15
