  SELECT DISTINCT
         t1.idPedido,
         t2.idVendedor

  FROM pedido as t1

  LEFT JOIN item_pedido as t2
  ON t1.idPedido = t2.idPedido

    WHERE t1.dtPedido < '2018-01-01'
        AND t1.dtPedido >= DATE('2018-01-01','-6 MONTH')
        AND t2.idVendedor IS NOT NULL
