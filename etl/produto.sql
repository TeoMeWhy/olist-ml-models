-- Databricks notebook source
WITH tb_join AS (

  SELECT DISTINCT
         t2.idVendedor,
         t3.*

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.produto as t3
  ON t2.idProduto = t3.idProduto

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND t2.idVendedor IS NOT NULL

),

tb_summary AS (SELECT 
    idVendedor,
    avg(coalesce(nrFotos,0)) as avg_fotos,
    avg(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as avg_volume_produto,
    percentile(vlComprimentoCm * vlAlturaCm * vlLarguraCm,0.5) as mediana_volume_produto,
    min(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as min_volume_produto,
    max(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as max_volume_produto,
    
   
    COUNT(DISTINCT CASE WHEN descCategoria='cama_mesa_banho' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS cama_mesa_banho_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='beleza_saude' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS beleza_saude_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='esporte_lazer' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS esporte_lazer_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='informatica_acessorios' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS informatica_acessorios_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='moveis_decoracao' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS moveis_decoracao_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='utilidades_domesticas' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS utilidades_domesticas_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='relogios_presentes' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS relogios_presentes_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='telefonia' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS telefonia_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='automotivo' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS automotivo_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='brinquedos' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS brinquedos_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='cool_stuff' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS cool_stuff_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='ferramentas_jardim' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS ferramentas_jardim_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='perfumaria' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS perfumaria_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='bebes' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS bebes_pct, 
    COUNT(DISTINCT CASE WHEN descCategoria='eletronicos' THEN idProduto ELSE 0 END)/COUNT(idProduto) AS eletronicos_pct 
    
    



FROM tb_join
group by idVendedor)

SELECT '2018-01-01' AS dtReference,
       *

FROM tb_summary

-- COMMAND ----------


