-- Databricks notebook source
-- MAGIC %md # Issues
-- MAGIC - Categorias vendidas
-- MAGIC - Quantidade de itens(SKU) distintos
-- MAGIC - Tamanho do produto (em volume)
-- MAGIC - Média de fotos por produto

-- COMMAND ----------

WITH tb_join AS (
  
  SELECT DISTINCT
         t2.idVendedor,
         t3.*
  FROM silver.olist.pedido AS t1
  
  LEFT JOIN silver.olist.item_pedido as t2
  ON t1.idPedido = t2.idPedido
  
  LEFT JOIN silver.olist.produto t3
  ON t2.idProduto = t3.idProduto
  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months ('2018-01-01', -6)
  AND t2.idVendedor IS NOT NULL
),

tb_summary AS(

  SELECT 
        idVendedor,
        avg(coalesce(nrFotos,0)) AS avgFotos,
        avg(vlComprimentoCm* vlAlturaCm * vlLarguraCm) as avgVolumeProdutoCm,
        percentile(vlComprimentoCm* vlAlturaCm * vlLarguraCm, 0.5) medianVolumeProduto,
        max(vlComprimentoCm* vlAlturaCm * vlLarguraCm) as minVolumeProdutoCm,
        min(vlComprimentoCm* vlAlturaCm * vlLarguraCm) as minVolumeProdutoCm,
        COUNT (DISTINCT CASE WHEN descCategoria = 'descCategoria'then idProduto end) / COUNT(DISTINCT idProduto) as pctdescCategoria,
        COUNT (DISTINCT CASE WHEN descCategoria = 'cama_mesa_banho'then idProduto end) / COUNT(DISTINCT idProduto) as pctcama_mesa_banho,
        COUNT (DISTINCT CASE WHEN descCategoria = 'beleza_saude'then idProduto end) / COUNT(DISTINCT idProduto) as pctbeleza_saude,
        COUNT (DISTINCT CASE WHEN descCategoria = 'esporte_lazer'then idProduto end) / COUNT(DISTINCT idProduto) as pctesporte_lazer,
        COUNT (DISTINCT CASE WHEN descCategoria = 'informatica_acessorios'then idProduto end) / COUNT(DISTINCT idProduto) as pctinformatica_acessorios,
        COUNT (DISTINCT CASE WHEN descCategoria = 'moveis_decoracao'then idProduto end) / COUNT(DISTINCT idProduto) as pctmoveis_decoracao,
        COUNT (DISTINCT CASE WHEN descCategoria = 'utilidades_domesticas'then idProduto end) / COUNT(DISTINCT idProduto) as pctutilidades_domesticas,
        COUNT (DISTINCT CASE WHEN descCategoria = 'relogios_presentes'then idProduto end) / COUNT(DISTINCT idProduto) as pctrelogios_presentes,
        COUNT (DISTINCT CASE WHEN descCategoria = 'telefonia'then idProduto end) / COUNT(DISTINCT idProduto) as pcttelefonia,
        COUNT (DISTINCT CASE WHEN descCategoria = 'automotivo'then idProduto end) / COUNT(DISTINCT idProduto) as pctautomotivo,
        COUNT (DISTINCT CASE WHEN descCategoria = 'brinquedos'then idProduto end) / COUNT(DISTINCT idProduto) as pctbrinquedos,
        COUNT (DISTINCT CASE WHEN descCategoria = 'cool_stuff'then idProduto end) / COUNT(DISTINCT idProduto) as pctcool_stuff,
        COUNT (DISTINCT CASE WHEN descCategoria = 'ferramentas_jardim'then idProduto end) / COUNT(DISTINCT idProduto) as pctferramentas_jardim,
        COUNT (DISTINCT CASE WHEN descCategoria = 'perfumaria'then idProduto end) / COUNT(DISTINCT idProduto) as pctperfumaria,
        COUNT (DISTINCT CASE WHEN descCategoria = 'bebes'then idProduto end) / COUNT(DISTINCT idProduto) as pctbebes,
        COUNT (DISTINCT CASE WHEN descCategoria = 'eletronicos'then idProduto end) / COUNT(DISTINCT idProduto) as pcteletronicos
        
  FROM tb_join 
  GROUP BY 1
)

select '2018-0-01' AS dtReference, *
FROM tb_summary



-- COMMAND ----------

-- Query aux

SELECT 
      descCategoria
  
  FROM silver.olist.item_pedido as t2
  
  LEFT JOIN silver.olist.produto t3
  ON t2.idProduto = t3.idProduto
  AND t2.idVendedor IS NOT NULL
  group by 1
  order by count(distinct idPedido) desc
  limit 15

