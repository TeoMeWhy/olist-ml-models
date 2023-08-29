WITH tb_pedidos AS (

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
    idVendedor IS NOT NULL

),

tb_pedidos_pagamentos AS (

  SELECT 
    t1.idVendedor,
    t2.*         
  FROM 
    tb_pedidos AS t1
  LEFT JOIN 
    silver.olist.pagamento_pedido AS t2
      ON t1.idPedido = t2.idPedido

),

tb_group AS (

  SELECT 
    idVendedor,
    descTipoPagamento,
    COUNT(distinct idPedido) as QtyPedidos,
    SUM(vlPagamento) as VolumePedido
  FROM 
    tb_pedidos_pagamentos
  GROUP BY 
    idVendedor, 
    descTipoPagamento
  ORDER BY 
    idVendedor, 
    descTipoPagamento

), 

tb_summary AS (
  SELECT 
    idVendedor,

    SUM(CASE WHEN descTipoPagamento = 'boleto' THEN QtyPedidos ELSE 0 END) AS qtde_boleto_pedido,
    SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN QtyPedidos ELSE 0 END) AS qtde_credit_card_pedido,
    SUM(CASE WHEN descTipoPagamento = 'voucher' THEN QtyPedidos ELSE 0 END) AS qtde_voucher_pedido,
    SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN QtyPedidos ELSE 0 END) AS qtde_debit_card_pedido,

    SUM(CASE WHEN descTipoPagamento = 'boleto' THEN QtyPedidos ELSE 0 END) / SUM(QtyPedidos) AS pct_qtd_boleto_pedido,
    SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN QtyPedidos ELSE 0 END) / SUM(QtyPedidos) AS pct_qtd_credit_card_pedido,
    SUM(CASE WHEN descTipoPagamento = 'voucher' THEN QtyPedidos ELSE 0 END) / SUM(QtyPedidos) AS pct_qtd_voucher_pedido,
    SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN QtyPedidos ELSE 0 END) / SUM(QtyPedidos) AS pct_qtd_debit_card_pedido,
 
    SUM(CASE WHEN descTipoPagamento = 'boleto' THEN VolumePedido ELSE 0 END) AS valor_boleto_pedido,
    SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN VolumePedido ELSE 0 END) AS valor_credit_card_pedido,
    SUM(CASE WHEN descTipoPagamento = 'voucher' THEN VolumePedido ELSE 0 END) AS valor_voucher_pedido,
    SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN VolumePedido ELSE 0 END) AS valor_debit_card_pedido,
  
    SUM(CASE WHEN descTipoPagamento = 'boleto' THEN VolumePedido ELSE 0 END) / SUM(VolumePedido) AS pct_valor_boleto_pedido,
    SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN VolumePedido ELSE 0 END) / SUM(VolumePedido) AS pct_valor_credit_card_pedido,
    SUM(CASE WHEN descTipoPagamento = 'voucher' THEN VolumePedido ELSE 0 END) / SUM(VolumePedido) AS pct_valor_voucher_pedido,
    SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN VolumePedido ELSE 0 END) / SUM(VolumePedido) AS pct_valor_debit_card_pedido
 
  FROM tb_group

  GROUP BY idVendedor), 

tb_card AS (
  SELECT 
    idVendedor, 
    AVG(nrParcelas) AS AvgParcelas, 
    MAX(nrParcelas) AS MaxParcelas,
    MIN(nrParcelas) AS MinParcelas, 
    PERCENTILE(nrParcelas, 0.5) AS MedParcelas 
  FROM 
    tb_pedidos_pagamentos
  WHERE 
    descTipoPagamento = 'credit_card'
  GROUP BY
   idVendedor 
) 

SELECT '{date}' AS DateRef, t1.*, t2.* EXCEPT(t2.idVendedor) 
FROM tb_summary t1 
LEFT JOIN tb_card t2 
ON t1.idVendedor = t2.idVendedor
