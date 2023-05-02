WITH 
    tb_join AS (
        SELECT t2.*,t3.idVendedor

        FROM pedido AS t1

        LEFT JOIN pagamento_pedido AS t2
        ON t1.idPedido = t2.idPedido

        LEFT JOIN item_pedido AS t3
        ON t1.idPedido = t3.idPedido

        WHERE t1.dtPedido <'2018-01-01'
        AND t1.dtPedido >= DATE('2018-01-01', '-6 months')
        AND t3.idVendedor IS NOT NULL
    ),

    tb_group AS (
        SELECT idVendedor,
                descTipoPagamento,
                count(DISTINCT idPedido) as qtdePedidoMeioPagamento,
                sum(vlPagamento) as vlPedidoMeioPagamento 
        FROM tb_join
        GROUP BY idVendedor,descTipoPagamento
        ORDER BY idVendedor,descTipoPagamento
    )

SELECT idVendedor,

SUM(CASE descTipoPagamento WHEN 'boleto' 
        THEN qtdePedidoMeioPagamento ELSE 0 END) as qtde_boleto_pedido,
SUM(CASE descTipoPagamento WHEN 'credit_card' 
        THEN qtdePedidoMeioPagamento ELSE 0 END) as qtd_credit_card_pedido,
SUM(CASE descTipoPagamento WHEN 'voucher' 
        THEN qtdePedidoMeioPagamento ELSE 0 END) as qtd_voucher_pedido,
SUM(CASE descTipoPagamento WHEN 'debit_card' 
        THEN qtdePedidoMeioPagamento ELSE 0 END) as qtd_debit_card_pedido,

SUM(CASE descTipoPagamento WHEN 'boleto' 
        THEN vlPedidoMeioPagamento ELSE 0 END) as valor_boleto_pedido,
SUM(CASE descTipoPagamento WHEN 'credit_card' 
        THEN vlPedidoMeioPagamento ELSE 0 END) as valor_credit_card_pedido,
SUM(CASE descTipoPagamento WHEN 'voucher' 
        THEN vlPedidoMeioPagamento ELSE 0 END) as valor_voucher_pedido,
SUM(CASE descTipoPagamento WHEN 'debit_card' 
        THEN vlPedidoMeioPagamento ELSE 0 END) as valor_debit_card_pedido,      

SUM(CASE descTipoPagamento WHEN 'boleto' 
        THEN qtdePedidoMeioPagamento ELSE 0 END)/SUM(qtdePedidoMeioPagamento) as pct_qtde_boleto_pedido,
SUM(CASE descTipoPagamento WHEN 'credit_card' 
        THEN qtdePedidoMeioPagamento ELSE 0 END)/SUM(qtdePedidoMeioPagamento) as pct_qtd_credit_card_pedido,
SUM(CASE descTipoPagamento WHEN 'voucher' 
        THEN qtdePedidoMeioPagamento ELSE 0 END)/SUM(qtdePedidoMeioPagamento) as pct_qtd_voucher_pedido,
SUM(CASE descTipoPagamento WHEN 'debit_card' 
        THEN qtdePedidoMeioPagamento ELSE 0 END)/SUM(qtdePedidoMeioPagamento) as pct_qtd_debit_card_pedido,

SUM(CASE descTipoPagamento WHEN 'boleto' 
        THEN vlPedidoMeioPagamento ELSE 0 END)/SUM(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
SUM(CASE descTipoPagamento WHEN 'credit_card' 
        THEN vlPedidoMeioPagamento ELSE 0 END)/SUM(vlPedidoMeioPagamento) as pct_valor_credit_card_pedido,
SUM(CASE descTipoPagamento WHEN 'voucher' 
        THEN vlPedidoMeioPagamento ELSE 0 END)/SUM(vlPedidoMeioPagamento) as pct_valor_voucher_pedido,
SUM(CASE descTipoPagamento WHEN 'debit_card' 
        THEN vlPedidoMeioPagamento ELSE 0 END)/SUM(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido      

FROM tb_group
GROUP BY 1