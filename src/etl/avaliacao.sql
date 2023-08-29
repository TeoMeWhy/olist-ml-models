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
    t1.dtPedido < '{date}'
  AND 
    t1.dtPedido >= ADD_MONTHS('{date}', -6)
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
  '{date}' AS DateRef,
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
  idVendedor