WITH tbl_pedido AS (
SELECT 
  t1.idPedido, 
  t2.idVendedor,
  t1.descSituacao, 
  t1.dtPedido,
  t1.dtAprovado,  
  t1.dtEnvio, 
  t1.dtEstimativaEntrega,
  t1.dtEntregue, 
  SUM(t2.vlFrete) AS TotalFrete 
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
  t2.idVendedor IS NOT NULL
GROUP BY 
  t1.idPedido, 
  t2.idVendedor,
  t1.descSituacao, 
  t1.dtPedido,
  t1.dtAprovado,  
  t1.dtEnvio, 
  t1.dtEstimativaEntrega,
  t1.dtEntregue) 

SELECT 
    '{date}' as DateRef, 
    idVendedor,
    COUNT(DISTINCT CASE WHEN  descSituacao  = 'canceled' THEN idPedido end) / COUNT (DISTINCT idPedido) AS pctPedidoCancelado, 
    COUNT(DISTINCT CASE WHEN  DATE(COALESCE(dtEntregue,'{date}')) > DATE(dtEstimativaEntrega) THEN idPedido END) / COUNT(DISTINCT CASE WHEN descSituacao  = 'delivered' THEN idPedido END) AS pctPedidoEntregueAtrasado, 
    AVG(TotalFrete) AS AvgFrete,
    PERCENTILE(TotalFrete, 0.50) AS MedianFrete, 
    MAX(TotalFrete) AS MaxFrete, 
    MIN(TotalFrete) AS MinFrete,
    AVG(DATEDIFF(COALESCE(dtEntregue,'{date}'), dtAprovado)) AS AvgDiasAprovadoEntrega,
    AVG(DATEDIFF(COALESCE(dtEntregue,'{date}'),dtPedido)) AS AvgDiasPedidoEntrega, 
    AVG(DATEDIFF(dtEstimativaEntrega,COALESCE(dtEntregue,'{date}'))) AS qntDiasEntregaPromessa
FROM 
  tbl_pedido
GROUP BY 
  idVendedor