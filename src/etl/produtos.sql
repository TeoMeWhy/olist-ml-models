WITH tbl_produtos AS
(SELECT 
  t2.idVendedor,
  t3.*
FROM 
  silver.olist.pedido t1 
LEFT JOIN 
  silver.olist.item_pedido t2 
    ON t1.idPedido = t2.idPedido 
LEFT JOIN 
  silver.olist.produto t3 
    ON t2.idProduto = t3.idProduto 
WHERE 
  dtPedido >= ADD_MONTHS('{date}', -6)
AND 
  dtPedido < '{date}'
AND 
  t2.idVendedor IS NOT NULL), 

tbl_tamanho_produto AS (
SELECT 
  idVendedor,
  AVG(COALESCE(nrFotos,0)) AS AvgFotos, 
  AVG(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS AvgVolumeProduto,
  MIN(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS MinVolumeProduto,
  MAX(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS MaxVolumeProduto,
  PERCENTILE(vlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) AS MedVolumeProduto 
FROM 
  tbl_produtos 
GROUP BY 
  idVendedor),

tbl_total_produtos AS( 
SELECT 
  idVendedor, 
  descCategoria, 
  COUNT(DISTINCT idProduto) AS TotalProducts 
FROM 
  tbl_produtos
GROUP BY 
  idVendedor, 
  descCategoria), 
 

tbl_categorias AS (
SELECT
  idVendedor, 
  SUM(CASE WHEN descCategoria = "cama_mesa_banho" THEN TotalProducts ELSE 0 END) / SUM(TotalProducts) AS perc_TotalProducts_cama_mesa_banho,
  SUM(CASE WHEN descCategoria = "beleza_saude" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_beleza_saude,
  SUM(CASE WHEN descCategoria = "esporte_lazer" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_esporte_lazer,
  SUM(CASE WHEN descCategoria = "informatica_acessorios" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_informatica_acessorios,
  SUM(CASE WHEN descCategoria = "moveis_decoracao" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_moveis_decoracao,
  SUM(CASE WHEN descCategoria = "utilidades_domesticas" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_utilidades_domesticas,
  SUM(CASE WHEN descCategoria = "relogios_presentes" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_relogios_presentes,
  SUM(CASE WHEN descCategoria = "telefonia" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_telefonia,
  SUM(CASE WHEN descCategoria = "automotivo" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_automotivo,
  SUM(CASE WHEN descCategoria = "brinquedos" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_brinquedos,
  SUM(CASE WHEN descCategoria = "cool_stuff" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_cool_stuff,
  SUM(CASE WHEN descCategoria = "ferramentas_jardim" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_ferramentas_jardim,
  SUM(CASE WHEN descCategoria = "perfumaria" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_perfumaria,
  SUM(CASE WHEN descCategoria = "bebes" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_bebes,
  SUM(CASE WHEN descCategoria = "eletronicos" THEN TotalProducts ELSE 0 END ) / SUM(TotalProducts) AS perc_TotalProducts_eletronicos
FROM 
 tbl_total_produtos
GROUP BY 
  idVendedor) 


SELECT 
  '{date}' AS DateRef, 
  t1.*, 
  t2.* EXCEPT (t2.idVendedor)
FROM 
  tbl_tamanho_produto t1 
LEFT JOIN 
  tbl_categorias t2 
    ON t1.idVendedor = t2.idVendedor
